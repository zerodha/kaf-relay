package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	RelayMetric = "kafka_relay_msg_count{source=\"%s\", destination=\"%s\", partition=\"%d\"}"

	ErrLaggingBehind = fmt.Errorf("topic end offset is lagging behind")
)

const (
	ModeFailover    = "failover"
	ModeSingle      = "single"
	IndefiniteRetry = -1
)

const (
	StateDisconnected = iota
	StateConnecting
)

func getCompressionCodec(codec string) kgo.CompressionCodec {
	switch codec {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	case "none":
		return kgo.NoCompression()

	default:
		return kgo.NoCompression()
	}
}

// getAckPolicy generates franz-go's commit ack for the given stream.CommitAck.
func getAckPolicy(ack string) kgo.Acks {
	switch ack {
	case "tcp":
		return kgo.NoAck()
	case "cluster":
		return kgo.AllISRAcks()
	case "leader":
		return kgo.LeaderAck()

	// fallback to default config in franz-go
	default:
		return kgo.AllISRAcks()
	}
}

// testConnection tests if the connection is active or not; Also confirms the existence of topics
func testConnection(client *kgo.Client, timeout time.Duration, topics []string) error {
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	req.AllowAutoTopicCreation = false
	req.IncludeClusterAuthorizedOperations = true
	req.IncludeTopicAuthorizedOperations = false

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	// Verify error code from requested topics response
	for _, topic := range resp.Topics {
		err = kerr.ErrorForCode(topic.ErrorCode)
		if err != nil {
			return err
		}
	}

	// force refresh client metadata info
	client.ForceMetadataRefresh()

	return nil
}

func createTLSConfig(ca, cl, key string) (kgo.Opt, error) {
	// Load the CA certificate
	caCert, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}

	// Load the client certificate and key
	clientCert, err := tls.LoadX509KeyPair(cl, key)
	if err != nil {
		return nil, err
	}

	// Load CA certificate into a certificate pool
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Set up TLS configuration
	return kgo.DialTLSConfig(&tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
	}), nil
}

func appendSASL(opts []kgo.Opt, cfg ClientCfg) []kgo.Opt {
	switch m := cfg.SASLMechanism; m {
	case SASLMechanismPlain:
		p := plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}
		opts = append(opts, kgo.SASL(p.AsMechanism()))

	case SASLMechanismScramSHA256, SASLMechanismScramSHA512:
		p := scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}

		mech := p.AsSha256Mechanism()
		if m == SASLMechanismScramSHA512 {
			mech = p.AsSha512Mechanism()
		}

		opts = append(opts, kgo.SASL(mech))
	}

	return opts
}

// leaveAndResetOffsets leaves the current consumer group and resets its offset if given.
func leaveAndResetOffsets(ctx context.Context, cl *kgo.Client, cfg ConsumerGroupCfg, offsets map[string]map[int32]kgo.Offset, l *slog.Logger) error {
	// leave group; mark the group as `Empty` before attempting to reset offsets.
	l.Debug("leaving group", "group_id", cfg.GroupID)
	err := cl.LeaveGroupContext(ctx)
	if err != nil {
		return err
	}

	// Reset consumer group offsets using the existing offsets
	if offsets != nil {
		l.Debug("resetting offsets", "offsets", offsets)
		if err := resetOffsets(ctx, cl, cfg, offsets, l); err != nil {
			return err
		}
	}

	return nil
}

// resetOffsets resets the consumer group with the given offsets map.
// Also waits for topics to catch up to the messages in case it is lagging behind.
func resetOffsets(ctx context.Context, cl *kgo.Client, cfg ConsumerGroupCfg, offsets map[string]map[int32]kgo.Offset, l *slog.Logger) error {
	var (
		maxAttempts = -1 // TODO: make this configurable?
		attempts    = 0
		admCl       = kadm.NewClient(cl)
	)

	// wait for topic lap to catch up
waitForTopicLag:
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if attempts >= maxAttempts && maxAttempts != IndefiniteRetry {
				return fmt.Errorf("max attempts(%d) for fetching offsets", maxAttempts)
			}

			// Get end offsets of the topics
			topicOffsets, err := admCl.ListEndOffsets(ctx, cfg.Topics...)
			if err != nil {
				l.Error("error fetching offsets", "err", err)
				return err
			}

			for t, po := range offsets {
				for p, o := range po {
					eO, ok := topicOffsets.Lookup(t, p)
					// TODO:
					if !ok {
						continue
					}

					if o.EpochOffset().Offset > eO.Offset {
						return fmt.Errorf("%w by %d msgs(s)", ErrLaggingBehind, o.EpochOffset().Offset-eO.Offset)
					}
				}
			}

			break waitForTopicLag
		}
	}

	// force set consumer group offsets
	commitOffsets := make(map[string]map[int32]kgo.EpochOffset)
	of := make(kadm.Offsets)
	for t, po := range offsets {
		oMap := make(map[int32]kgo.EpochOffset)
		for p, o := range po {
			oMap[p] = o.EpochOffset()
			of.AddOffset(t, p, o.EpochOffset().Offset, -1)
		}
		commitOffsets[t] = oMap
	}
	cl.SetOffsets(commitOffsets)

	l.Info("resetting offsets for consumer group",
		"broker", cfg.BootstrapBrokers, "group", cfg.GroupID, "offsets", of)
	resp, err := admCl.CommitOffsets(ctx, cfg.GroupID, of)
	if err != nil {
		l.Error("error resetting group offset", "err", err)
	}

	_ = resp
	// // check for errors in offset responses
	// for _, or := range resp {
	// 	for _, r := range or {
	// 		if r.Err != nil {
	// 			l.Error("error resetting group offset", "err", r.Err)
	// 			return err
	// 		}
	// 	}
	// }

	return nil
}

// retryBackoff is basic backoff fn from franz-go
// ref: https://github.com/twmb/franz-go/blob/01651affd204d4a3577a341e748c5d09b52587f8/pkg/kgo/go#L450
func retryBackoff() func(int) time.Duration {
	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(fails int) time.Duration {
		const (
			min = 1 * time.Second
			max = 20 * time.Second / 2
		)
		if fails <= 0 {
			return min
		}
		if fails > 10 {
			return max
		}

		backoff := min * time.Duration(1<<(fails-1))

		rngMu.Lock()
		jitter := 0.8 + 0.4*rng.Float64()
		rngMu.Unlock()

		backoff = time.Duration(float64(backoff) * jitter)

		if backoff > max {
			return max
		}
		return backoff
	}
}

// waitTries waits for the timer to hit for the deadline with the backoff duration
func waitTries(ctx context.Context, b time.Duration) {
	deadline := time.Now().Add(b)
	after := time.NewTimer(time.Until(deadline))
	select {
	case <-ctx.Done():
		return
	case <-after.C:
	}
}

// thresholdExceeded checks if the difference between the offsets is breaching the threshold
func thresholdExceeded(offsetsX, offsetsY kadm.ListedOffsets, max int64) bool {
	for t, po := range offsetsX {
		for p, x := range po {
			y, ok := offsetsY.Lookup(t, p)
			if !ok {
				continue
			}

			// check if the difference is breaching threshold
			if y.Offset < x.Offset {
				if (x.Offset - y.Offset) >= max {
					return true
				}
			}
		}
	}

	return false
}

// getEndOffsets returns the end offsets of the given topics
func getEndOffsets(ctx context.Context, client *kgo.Client, topics []string, timeout time.Duration) (kadm.ListedOffsets, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	adm := kadm.NewClient(client)
	offsets, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("error listing end offsets of topics(%v): %v", topics, err)
	}

	return offsets, nil
}

// hasReachedEnd reports if there is any pending messages in given topic-partition
func hasReachedEnd(offsets map[string]map[int32]int64) bool {
	for _, p := range offsets {
		for _, o := range p {
			if o > 0 {
				return false
			}
		}
	}

	return true
}

// Node represents a node with its weight
type Node struct {
	Down   bool
	ID     int
	Weight int
}

// NodeTracker keeps track of the healthiest node
type NodeTracker struct {
	sync.Mutex

	nodes      []Node
	healthiest Node
}

// NewNodeTracker creates a new HealthiestNodeTracker
func NewNodeTracker(nodes []Node) *NodeTracker {
	return &NodeTracker{
		nodes:      nodes,
		healthiest: Node{Down: true, Weight: -1},
	}
}

// GetHealthy returns the healthiest node without blocking
func (h *NodeTracker) GetHealthy() Node {
	h.Lock()
	node := h.healthiest
	h.Unlock()

	return node
}

func (h *NodeTracker) PushUp(nodeID int, weight int) {
	h.updateWeight(nodeID, weight, false)
}

func (h *NodeTracker) PushDown(nodeID int) {
	h.updateWeight(nodeID, -1, true)
}

// updateWeight updates the weight for a particular node
func (h *NodeTracker) updateWeight(nodeID int, weight int, down bool) {
	h.Lock()
	defer h.Unlock()

	var (
		minWeight = -1
	)
	for i := 0; i < len(h.nodes); i++ {
		node := h.nodes[i]
		if node.ID == nodeID {
			node.Down = down
			node.Weight = weight
		}
		h.nodes[i] = node

		if node.Weight > minWeight && !node.Down {
			minWeight = node.Weight
			h.healthiest = node
		}
	}
}
