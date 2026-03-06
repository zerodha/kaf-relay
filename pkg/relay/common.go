package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
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

const RelayMetricPrefix = "kafka_relay_"

// Metric names. Target{} implementations should use these to emit metrics.
const (
	MetricSourceErrors      = "source_errors_total"
	MetricSourceUnhealthy   = "source_unhealthy_total"
	MetricSourceKafkaErrors = "source_kafka_errors_total"
	MetricSourceHighwater   = "source_highwatermark"
	MetricTargetErrors      = "target_errors_total"
	MetricTargetKafkaErrors = "target_kafka_errors_total"
	MetricMsgsRelayed       = "messages_relayed_total"

	MetricFilteredMsgs         = "messages_filtered_total"
	MetricInletBlocks          = "target_inlet_blocks_total"
	MetricFlushBatchSize       = "target_flush_batch_size"
	MetricFlushDuration        = "target_flush_duration_seconds"
	MetricFlushRetries         = "target_flush_retries_total"
	MetricCandidateSwitches    = "source_candidate_switches_total"
	MetricSourceConnections    = "source_connections_total"
	MetricLagThresholdExceeded = "source_lag_threshold_exceeded_total"
)

// Canonical error labels for metrics.
const (
	ErrLabelConnectionFailed   = "connection_failed"
	ErrLabelClientClosed       = "client_closed"
	ErrLabelFetch              = "fetch_error"
	ErrLabelTLSConfig          = "tls_config_error"
	ErrLabelClientCreation     = "client_creation_failed"
	ErrLabelProducerConnection = "connection_failed"
	ErrLabelProduce            = "produce_failed"
	ErrLabelProduceRetries     = "produce_retries_exhausted"
)

var (
	ErrLaggingBehind = fmt.Errorf("topic end offset is lagging behind")
)

// MetricName builds a Prometheus metric name with optional key=value label pairs.
// E.g. MetricName("errors_total", "node_id", "1") → `kafka_relay_errors_total{node_id="1"}`
func MetricName(base string, labels ...string) string {
	if len(labels) == 0 {
		return RelayMetricPrefix + base
	}

	var b strings.Builder
	b.WriteString(RelayMetricPrefix)
	b.WriteString(base)
	b.WriteByte('{')
	for i := 0; i < len(labels); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(labels[i])
		b.WriteString(`="`)
		b.WriteString(labels[i+1])
		b.WriteByte('"')
	}
	b.WriteByte('}')

	return b.String()
}

const (
	SASLMechanismPlain       = "PLAIN"
	SASLMechanismScramSHA256 = "SCRAM-SHA-256"
	SASLMechanismScramSHA512 = "SCRAM-SHA-512"
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

func GetCompressionCodec(codec string) kgo.CompressionCodec {
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

// GetAckPolicy generates franz-go's commit ack for the given stream.CommitAck.
func GetAckPolicy(ack string) kgo.Acks {
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

// ValidateConn tests if the connection is active and confirms the existence of topics
// (and optionally, specific partitions).
func ValidateConn(client *kgo.Client, timeout time.Duration, topics []string, partitions map[string]uint) error {
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

		if partitions == nil {
			continue
		}

		p, ok := partitions[*topic.Topic]
		if !ok {
			continue
		}

		found := false
		for _, t := range topic.Partitions {
			if uint(t.Partition) == p {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("partition %d not found for topic %s", p, *topic.Topic)
		}

	}

	// force refresh client metadata info
	client.ForceMetadataRefresh()

	return nil
}

func GetTLSConfig(ca, cl, key string) (kgo.Opt, error) {
	// Load the CA certificate.
	caCert, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}

	// Load the client certificate and key.
	clientCert, err := tls.LoadX509KeyPair(cl, key)
	if err != nil {
		return nil, err
	}

	// Load CA certificate into a certificate pool.
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Set up TLS configuration.
	return kgo.DialTLSConfig(&tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
	}), nil
}

func AddSASLConfig(opts []kgo.Opt, cfg KafkaCfg) []kgo.Opt {
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

// GetBackoffFn returns a backoff function based on the config.
func GetBackoffFn(enabled bool, min, max time.Duration) func(int) time.Duration {
	if enabled {
		return retryBackoff(min, max)
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

// WaitTries waits for the given backoff duration, respecting context cancellation.
func WaitTries(ctx context.Context, waitDuration time.Duration) {
	if waitDuration == 0 {
		return
	}

	deadline := time.Now().Add(waitDuration)
	after := time.NewTimer(time.Until(deadline))
	defer after.Stop()

	select {
	case <-ctx.Done():
		return
	case <-after.C:
	}
}

// checkNetErr checks if the given error is a network error.
func checkNetErr(err error) bool {
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

// CheckTCP dials a TCP address and checks if it's reachable.
func CheckTCP(ctx context.Context, addrs []string, timeout time.Duration) bool {
	d := net.Dialer{Timeout: timeout}
	up := false
	for _, addr := range addrs {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil && checkNetErr(err) {
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

// GetHighWatermark returns the highest watermark / offsets for the given topics as of the moment of requesting.
func GetHighWatermark(ctx context.Context, client *kgo.Client, topics []string, timeout time.Duration) (kadm.ListedOffsets, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	adm := kadm.NewClient(client)
	offsets, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("error listing end offsets of topic(%v): %v", topics, err)
	}

	return offsets, nil
}
