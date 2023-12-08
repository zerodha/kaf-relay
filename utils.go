package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
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

const (
	ModeFailover = "failover"

	StateConnected = iota - 1
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

func inSlice(x string, l []string) bool {
	for _, i := range l {
		if i == x {
			return true
		}
	}

	return false
}

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

// retryBackoff is basic backoff fn from franz-go
// ref: https://github.com/twmb/franz-go/blob/01651affd204d4a3577a341e748c5d09b52587f8/pkg/kgo/config.go#L450
func retryBackoff() func(int) time.Duration {
	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(fails int) time.Duration {
		const (
			min = 500 * time.Millisecond
			max = 10 * time.Second / 2
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

func getCommittedOffsets(ctx context.Context, client *kgo.Client, topics []string) (kadm.ListedOffsets, error) {
	adm := kadm.NewClient(client)
	offsets, err := adm.ListCommittedOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("error listing committed offsets of topics(%v): %v", topics, err)
	}

	return offsets, nil
}

func getEndOffsets(ctx context.Context, client *kgo.Client, topics []string) (kadm.ListedOffsets, error) {
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
