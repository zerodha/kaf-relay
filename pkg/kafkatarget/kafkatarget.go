// Package kafkatarget implements relay.Target for a target Kafka cluster.
// It handles batching, flushing, and retries when producing to Kafka.
package kafkatarget

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

type targetMetrics struct {
	networkErrClientCreation *metrics.Counter
	networkErrConnection     *metrics.Counter
	kafkaErrTLS              *metrics.Counter
	kafkaErrProduce          *metrics.Counter
	kafkaErrProduceRetries   *metrics.Counter
	flushBatchSize           *metrics.Histogram
	flushDuration            *metrics.Histogram
	flushRetries             *metrics.Counter
	inletBlocks              *metrics.Counter
}

func newTargetMetrics(set *metrics.Set) targetMetrics {
	return targetMetrics{
		networkErrClientCreation: set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetErrors, "error", relay.ErrLabelClientCreation)),
		networkErrConnection:     set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetErrors, "error", relay.ErrLabelProducerConnection)),
		kafkaErrTLS:              set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetKafkaErrors, "error", relay.ErrLabelTLSConfig)),
		kafkaErrProduce:          set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetKafkaErrors, "error", relay.ErrLabelProduce)),
		kafkaErrProduceRetries:   set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetKafkaErrors, "error", relay.ErrLabelProduceRetries)),
		flushBatchSize:           set.GetOrCreateHistogram(relay.MetricName(relay.MetricFlushBatchSize)),
		flushDuration:            set.GetOrCreateHistogram(relay.MetricName(relay.MetricFlushDuration)),
		flushRetries:             set.GetOrCreateCounter(relay.MetricName(relay.MetricFlushRetries)),
		inletBlocks:              set.GetOrCreateCounter(relay.MetricName(relay.MetricInletBlocks)),
	}
}

// Target is a relay.Target implementation that produces messages to a target Kafka cluster.
type Target struct {
	client *kgo.Client
	cfg    relay.TargetCfg
	pCfg   relay.ProducerCfg
	ctx    context.Context
	metr   targetMetrics
	log    *slog.Logger

	topics relay.Topics

	// inletCh receives messages for batching and flushing.
	inletCh chan *kgo.Record

	// Holds the active batch that is produced to destination topic.
	batch []*kgo.Record

	// doneCh is closed when Start() returns, so Close() can wait for drain.
	doneCh chan struct{}
}

// New creates a new Kafka target that implements relay.Target.
// It connects to the target Kafka cluster and validates that all target topics exist.
func New(cfg relay.TargetCfg, pCfg relay.ProducerCfg, topics relay.Topics, ctx context.Context, m *metrics.Set, log *slog.Logger) (*Target, error) {
	tg := &Target{
		cfg:    cfg,
		pCfg:   pCfg,
		ctx:    ctx,
		metr:   newTargetMetrics(m),
		log:    log,
		topics: topics,

		batch:   make([]*kgo.Record, 0, pCfg.BatchSize),
		inletCh: make(chan *kgo.Record, pCfg.BufferSize),
		doneCh:  make(chan struct{}),
	}

	cl, err := tg.initProducer(topics)
	if err != nil {
		return nil, err
	}

	tg.client = cl
	return tg, nil
}

// GetHighWatermark returns the current end offsets on the target Kafka topics,
// converted to the generic relay.Offsets type.
func (tg *Target) GetHighWatermark(ctx context.Context) (relay.Offsets, error) {
	var topicNames []string
	for _, t := range tg.topics {
		topicNames = append(topicNames, t.TargetTopic)
	}

	listed, err := relay.GetHighWatermark(ctx, tg.client, topicNames, tg.cfg.ReqTimeout)
	if err != nil {
		return nil, err
	}

	// Convert kadm.ListedOffsets → relay.Offsets (plain int64 offsets).
	out := make(relay.Offsets)
	listed.Each(func(lo kadm.ListedOffset) {
		if _, ok := out[lo.Topic]; !ok {
			out[lo.Topic] = make(map[int32]int64)
		}
		out[lo.Topic][lo.Partition] = lo.Offset
	})

	return out, nil
}

// Start runs the blocking batch/flush loop.
// It reads messages from the internal channel, batches them, and flushes
// to Kafka periodically or when the batch is full.
// Returns when Close() is called (which closes inletCh) or on fatal error.
//
// Uses its own context.Background() — deliberately detached from the global
// context so that in-flight flushes can complete even during shutdown.
func (tg *Target) Start() error {
	defer close(tg.doneCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tick := time.NewTicker(tg.pCfg.FlushFrequency)
	defer tick.Stop()

	// Final flush after the loop exits.
	defer func() {
		if err := tg.client.Flush(ctx); err != nil {
			tg.log.Error("error while flushing the producer", "err", err)
		}
	}()

	for {
		select {
		// Queue the message and flush if the batch size is reached.
		case msg, ok := <-tg.inletCh:
			if !ok {
				// Channel closed by Close() — drain remaining messages.
				if err := tg.drain(); err != nil {
					return err
				}
				return nil
			}

			tg.batch = append(tg.batch, msg)
			if len(tg.batch) >= tg.pCfg.FlushBatchSize {
				// flush() only returns an error if retries are exhausted (configured via max_retries).
				// For long-running daemons, set max_retries to -1 (indefinite).
				if err := tg.flush(ctx); err != nil {
					return err
				}
				tick.Reset(tg.pCfg.FlushFrequency)
			}

		// Flush the batch at a regular interval.
		case <-tick.C:
			if len(tg.batch) > 0 {
				if err := tg.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// Write converts a relay.Message to a kgo.Record and enqueues it for production.
// Blocks if the internal buffer is full.
func (tg *Target) Write(ctx context.Context, msg relay.Message) error {
	rec := &kgo.Record{
		Key:   msg.Key,
		Value: msg.Value,
		Topic: msg.Topic,
	}

	// -1 means auto-partition (let the partitioner decide).
	if msg.Partition >= 0 {
		rec.Partition = msg.Partition
	}

	for _, h := range msg.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	// Non-blocking attempt first; if the channel is full, log and block.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case tg.inletCh <- rec:
	default:
		tg.metr.inletBlocks.Inc()
		tg.log.Error("target inlet channel blocked")
		// Blocking send — backpressure from the flush loop.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tg.inletCh <- rec:
		}
	}

	return nil
}

// Close signals the target to stop accepting new messages, drain
// any buffered messages, and shut down. Blocks until Start() returns.
func (tg *Target) Close() error {
	close(tg.inletCh)

	// Wait for Start() to finish draining.
	<-tg.doneCh

	if tg.client != nil {
		// Prevent blocking on close.
		tg.client.PurgeTopicsFromProducing()
	}
	return nil
}

// initProducer creates the Kafka producer client with retries.
func (tg *Target) initProducer(top relay.Topics) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.ProduceRequestTimeout(tg.pCfg.SessionTimeout),
		kgo.RecordDeliveryTimeout(tg.pCfg.SessionTimeout),
		kgo.ProducerBatchMaxBytes(int32(tg.pCfg.MaxMessageBytes)),
		kgo.MaxBufferedRecords(tg.pCfg.FlushBatchSize),
		kgo.ProducerLinger(tg.pCfg.FlushFrequency),
		kgo.ProducerBatchCompression(relay.GetCompressionCodec(tg.pCfg.Compression)),
		kgo.SeedBrokers(tg.pCfg.BootstrapBrokers...),
		kgo.RequiredAcks(relay.GetAckPolicy(tg.pCfg.CommitAck)),
	}

	// TCPAck/LeaderAck requires Kafka deduplication to be turned off.
	if !tg.pCfg.EnableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	if tg.pCfg.EnableAuth {
		opts = relay.AddSASLConfig(opts, tg.pCfg.KafkaCfg)
	}

	if tg.pCfg.EnableTLS {
		if tg.pCfg.CACertPath == "" && tg.pCfg.ClientCertPath == "" && tg.pCfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := relay.GetTLSConfig(tg.pCfg.CACertPath, tg.pCfg.ClientCertPath, tg.pCfg.ClientKeyPath)
			if err != nil {
				tg.metr.kafkaErrTLS.Inc()
				return nil, err
			}
			opts = append(opts, tlsOpt)
		}
	}

	var (
		retries = 0
		backoff = relay.GetBackoffFn(tg.cfg.EnableBackoff, tg.cfg.BackoffMin, tg.cfg.BackoffMax)
		err     error
		cl      *kgo.Client
	)

	// Retry until a successful connection.
outerLoop:
	for retries < tg.pCfg.MaxRetries || tg.pCfg.MaxRetries == relay.IndefiniteRetry {
		select {
		case <-tg.ctx.Done():
			break outerLoop
		default:
			cl, err = kgo.NewClient(opts...)
			if err != nil {
				tg.metr.networkErrClientCreation.Inc()
				tg.log.Error("error creating producer client", "error", err)
				retries++
				relay.WaitTries(tg.ctx, backoff(retries))
				continue
			}

			// Get the target (producer) topics.
			var (
				topics     []string
				partitions = map[string]uint{}
			)
			for _, t := range top {
				topics = append(topics, t.TargetTopic)
				if !t.AutoTargetPartition {
					partitions[t.TargetTopic] = t.TargetPartition
				}
			}

			// Test connectivity and ensure destination topics exist.
			if err := relay.ValidateConn(cl, tg.pCfg.SessionTimeout, topics, partitions); err != nil {
				cl.Close()

				tg.metr.networkErrConnection.Inc()
				tg.log.Error("error connecting to producer", "err", err)
				retries++
				relay.WaitTries(tg.ctx, backoff(retries))
				continue
			}

			break outerLoop
		}
	}

	if err != nil {
		return nil, err
	}

	return cl, nil
}

// drain drains and flushes any pending messages in the producer.
func (tg *Target) drain() error {
	now := time.Now()
	for rec := range tg.inletCh {
		tg.batch = append(tg.batch, rec)
	}

	ct := len(tg.batch)
	if ct > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), tg.pCfg.SessionTimeout)
		defer cancel()

		if err := tg.flush(ctx); err != nil {
			return err
		}
	}

	tg.log.Debug("flushing producer batch", "elapsed", time.Since(now).Seconds(), "count", ct)
	tg.batch = tg.batch[:0]
	return nil
}

// flush flushes the batch to Kafka with retries. Only returns an error
// if retries are exhausted (configured via [target].max_retries; set to
// -1 for indefinite retries in long-running daemons).
func (tg *Target) flush(ctx context.Context) error {
	start := time.Now()
	var (
		retries = 0
		sent    bool
		backOff = relay.GetBackoffFn(tg.cfg.EnableBackoff, tg.cfg.BackoffMin, tg.cfg.BackoffMax)
	)

retry:
	for retries < tg.pCfg.MaxRetries || tg.pCfg.MaxRetries == relay.IndefiniteRetry {
		batchLen := len(tg.batch)
		tg.metr.flushBatchSize.Update(float64(batchLen))

		// Check that the destination cluster is healthy before retrying.
		if retries > 0 && !relay.CheckTCP(ctx, tg.pCfg.BootstrapBrokers, tg.pCfg.SessionTimeout) {
			continue
		}

		tg.log.Debug("producing message", "broker", tg.client.OptValue(kgo.SeedBrokers), "msgs", batchLen, "retry", retries)
		results := tg.client.ProduceSync(ctx, tg.batch...)

		var (
			err      error
			failures []*kgo.Record
		)
		for idx, res := range results {
			if res.Err == context.Canceled {
				return ctx.Err()
			}

			if res.Err != nil {
				// Reset context/timestamp so the record can be produced again.
				tg.batch[idx].Timestamp = time.Time{}
				tg.batch[idx].Context = nil

				failures = append(failures, tg.batch[idx])
				err = res.Err
				continue
			}
		}

		tg.log.Debug("produced last offset", "offset", results[len(results)-1].Record.Offset, "batch", batchLen, "retry", retries)

		if err != nil {
			tg.metr.kafkaErrProduce.Inc()
			tg.log.Error("error producing message", "err", err, "failed_count", batchLen, "retry", retries)

			bufRecs := tg.client.BufferedProduceRecords()
			if bufRecs > 0 {
				if err := tg.client.AbortBufferedRecords(ctx); err != nil {
					tg.log.Error("error aborting buffered records", "err", err)
				} else {
					tg.log.Debug("aborted buffered records, retrying failed messages", "recs", bufRecs)
				}
			}

			tg.batch = tg.batch[:0]
			tg.batch = failures
			retries++
			tg.metr.flushRetries.Inc()

			b := backOff(retries)
			tg.log.Debug("error producing message; waiting for retry...", "backoff", b.Seconds())
			relay.WaitTries(ctx, b)

			continue retry
		}

		tg.batch = tg.batch[batchLen:]
		sent = true
		break retry
	}

	if !sent {
		tg.metr.kafkaErrProduceRetries.Inc()
		return fmt.Errorf("error producing message; exhausted retries (%v)", tg.pCfg.MaxRetries)
	}

	tg.metr.flushDuration.UpdateDuration(start)
	return nil
}
