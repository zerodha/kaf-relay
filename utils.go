package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"sync"
	"syscall"
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
	if err := leaveGroup(ctx, cl, cfg); err != nil {
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

// leaveGroup leaves the consumer group with our instance id
func leaveGroup(ctx context.Context, cl *kgo.Client, cfg ConsumerGroupCfg) error {
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = cfg.GroupID

	req.Members = []kmsg.LeaveGroupRequestMember{{InstanceID: &instanceID}}
	_, err := req.RequestWith(ctx, cl)
	return err
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
	of := make(kadm.Offsets)
	for t, po := range offsets {
		oMap := make(map[int32]kgo.EpochOffset)
		for p, o := range po {
			oMap[p] = o.EpochOffset()
			of.AddOffset(t, p, o.EpochOffset().Offset, -1)
		}
	}

	l.Info("resetting offsets for consumer group",
		"broker", cfg.BootstrapBrokers, "group", cfg.GroupID, "offsets", of)
	resp, err := admCl.CommitOffsets(ctx, cfg.GroupID, of)
	if err != nil {
		l.Error("error resetting group offset", "err", err)
	}

	if err := resp.Error(); err != nil {
		l.Error("error resetting group offset", "err", err)
	}

	// _ = resp
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

// getBackoffFn returns the backoff function based on the config
func getBackoffFn(cfg BackoffCfg) func(int) time.Duration {
	if cfg.Enable {
		return retryBackoff(cfg.Min, cfg.Max)
	} else {
		return func(int) time.Duration {
			return 0
		}
	}
}

// retryBackoff is basic backoff fn from franz-go
// ref: https://github.com/twmb/franz-go/blob/01651affd204d4a3577a341e748c5d09b52587f8/pkg/kgo/go#L450
func retryBackoff(min, max time.Duration) func(int) time.Duration {
	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(fails int) time.Duration {
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

// checkErr checks if the given error is a network error or not
func checkErr(err error) bool {
	if netError, ok := err.(net.Error); ok && netError.Timeout() {
		return true
	}

	switch t := err.(type) {
	case *net.OpError:
		if t.Op == "dial" {
			return true
		} else if t.Op == "read" {
			return true
		}

	case syscall.Errno:
		if t == syscall.ECONNREFUSED {
			return true
		}
	}

	return false
}

// healthcheck checks if the given address is reachable or not
func healthcheck(ctx context.Context, addrs []string, timeout time.Duration) bool {
	d := net.Dialer{Timeout: timeout}
	up := false
	for _, addr := range addrs {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil && checkErr(err) {
			continue
		}

		if conn != nil {
			conn.Close()
		}

		// atleast one address is reachable
		ok := err == nil
		if ok {
			up = true
			break
		}
	}

	return up
}
