// Package kafkatarget implements relay.Target for a target Kafka cluster.
// It handles batching, flushing, and retries when producing to Kafka.
package kafkatarget

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
		networkErrClientCreation: set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetErrors, relay.Label{Key: "error", Value: relay.ErrLabelClientCreation})),
		networkErrConnection:     set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetErrors, relay.Label{Key: "error", Value: relay.ErrLabelProducerConnection})),
		kafkaErrTLS:              set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetKafkaErrors, relay.Label{Key: "error", Value: relay.ErrLabelTLSConfig})),
		kafkaErrProduce:          set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetKafkaErrors, relay.Label{Key: "error", Value: relay.ErrLabelProduce})),
		kafkaErrProduceRetries:   set.GetOrCreateCounter(relay.MetricName(relay.MetricTargetKafkaErrors, relay.Label{Key: "error", Value: relay.ErrLabelProduceRetries})),
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
	metr   targetMetrics
	log    *slog.Logger

	topics relay.Topics

	// inletCh receives messages for batching and flushing.
	inletCh chan *kgo.Record

	// Holds the active batch that is produced to destination topic.
	batch []*kgo.Record

	// doneCh is closed when Start() returns, so Close() can wait for drain.
	doneCh    chan struct{}
	closeCh   chan struct{}
	startedCh chan struct{}

	closeOnce    sync.Once
	stopOnce     sync.Once
	clientOnce   sync.Once
	doneOnce     sync.Once
	lifecycleMu  sync.Mutex
	started      bool
	closed       bool
	workerCancel context.CancelFunc
	runErr       error
	writers      sync.WaitGroup
	sendMu       sync.Mutex
}

var errTargetClosed = errors.New("target is closed")

// mixedPartitioner preserves explicit relay partition assignments while
// delegating records marked with partition -1 to Kafka's normal partitioning
// strategy.
type mixedPartitioner struct {
	automatic kgo.Partitioner
}

func (p mixedPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return mixedTopicPartitioner{
		automatic: p.automatic.ForTopic(topic),
	}
}

type mixedTopicPartitioner struct {
	automatic kgo.TopicPartitioner
}

func (p mixedTopicPartitioner) RequiresConsistency(record *kgo.Record) bool {
	if record.Partition >= 0 {
		return true
	}
	return p.automatic.RequiresConsistency(record)
}

func (p mixedTopicPartitioner) Partition(record *kgo.Record, partitions int) int {
	if record.Partition >= 0 {
		return int(record.Partition)
	}
	return p.automatic.Partition(record, partitions)
}

// New creates a new Kafka target that implements relay.Target.
// It connects to the target Kafka cluster and validates that all target topics exist.
// The provided ctx controls cancellation of the initial connection/retry loop.
func New(ctx context.Context, cfg relay.TargetCfg, pCfg relay.ProducerCfg, topics relay.Topics, m *metrics.Set, log *slog.Logger) (*Target, error) {
	tg := &Target{
		cfg:    cfg,
		pCfg:   pCfg,
		metr:   newTargetMetrics(m),
		log:    log,
		topics: topics,

		batch:     make([]*kgo.Record, 0, pCfg.BatchSize),
		inletCh:   make(chan *kgo.Record, pCfg.BufferSize),
		doneCh:    make(chan struct{}),
		closeCh:   make(chan struct{}),
		startedCh: make(chan struct{}),
	}

	cl, err := tg.initProducer(ctx, topics)
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
// Returns when Close() is called or on fatal error.
//
// Uses its own context.Background() — deliberately detached from the global
// context so that in-flight flushes can complete even during shutdown.
func (tg *Target) Start() (runErr error) {
	tg.lifecycleMu.Lock()
	if tg.closed {
		tg.lifecycleMu.Unlock()
		tg.markDone()
		return nil
	}
	if tg.started {
		tg.lifecycleMu.Unlock()
		return fmt.Errorf("target already started")
	}
	tg.started = true
	ctx, cancel := context.WithCancel(context.Background())
	tg.workerCancel = cancel
	tg.lifecycleMu.Unlock()
	close(tg.startedCh)
	defer func() {
		tg.stop()
		tg.writers.Wait()
		cancel()
		tg.lifecycleMu.Lock()
		tg.workerCancel = nil
		tg.runErr = runErr
		tg.lifecycleMu.Unlock()
		tg.markDone()
	}()
	tick := time.NewTicker(tg.pCfg.FlushFrequency)
	defer tick.Stop()

	// Final flush after the loop exits. Preserve an earlier worker error if one
	// already caused the loop to stop.
	defer func() {
		if err := tg.client.Flush(ctx); err != nil {
			tg.log.Error("error while flushing the producer", "err", err)
			if runErr == nil {
				runErr = err
			}
		}
	}()
	defer func() {
		tg.stop()
		tg.writers.Wait()
	}()

	for {
		select {
		// Queue the message and flush if the batch size is reached.
		case msg := <-tg.inletCh:
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

		case <-tg.closeCh:
			tg.writers.Wait()
			return tg.drain(ctx)
		}
	}
}

// Write converts a relay.Message to a kgo.Record and enqueues it for production.
// Blocks if the internal buffer is full.
func (tg *Target) Write(ctx context.Context, msg relay.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tg.closeCh:
		return errTargetClosed
	case <-tg.startedCh:
	}

	rec := &kgo.Record{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: -1,
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

	for {
		tg.sendMu.Lock()
		tg.lifecycleMu.Lock()
		if tg.closed {
			tg.lifecycleMu.Unlock()
			tg.sendMu.Unlock()
			return errTargetClosed
		}
		tg.writers.Add(1)
		tg.lifecycleMu.Unlock()

		select {
		case tg.inletCh <- rec:
			tg.sendMu.Unlock()
			defer tg.writers.Done()
			return nil
		default:
			tg.writers.Done()
			tg.sendMu.Unlock()
		}

		tg.metr.inletBlocks.Inc()
		tg.log.Error("target inlet channel blocked")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tg.closeCh:
			return errTargetClosed
		case <-time.After(time.Millisecond):
		}
	}
}

// Close signals the target to stop accepting new messages, drain
// any buffered messages, and shut down. Blocks until Start() returns.
// Safe to call multiple times; only the first call has effect.
func (tg *Target) Close() error {
	tg.closeOnce.Do(func() {
		tg.stop()
		tg.lifecycleMu.Lock()
		started := tg.started
		tg.lifecycleMu.Unlock()
		if !started {
			tg.markDone()
		}
	})

	// Wait for Start() to finish draining. A long-running retry loop gets a
	// bounded grace period before its worker context is canceled.
	tg.lifecycleMu.Lock()
	started := tg.started
	workerCancel := tg.workerCancel
	tg.lifecycleMu.Unlock()
	if started {
		shutdownTimeout := tg.pCfg.SessionTimeout
		if shutdownTimeout <= 0 {
			shutdownTimeout = 5 * time.Second
		}
		timer := time.NewTimer(shutdownTimeout)
		select {
		case <-tg.doneCh:
			timer.Stop()
		case <-timer.C:
			if workerCancel != nil {
				workerCancel()
			}
			<-tg.doneCh
		}
	} else {
		<-tg.doneCh
	}

	if tg.client != nil {
		tg.clientOnce.Do(func() {
			// Prevent blocking on close.
			tg.client.PurgeTopicsFromProducing()
			tg.client.Close()
		})
	}

	tg.lifecycleMu.Lock()
	runErr := tg.runErr
	tg.lifecycleMu.Unlock()
	return runErr
}

func (tg *Target) stop() {
	tg.stopOnce.Do(func() {
		tg.sendMu.Lock()
		tg.lifecycleMu.Lock()
		tg.closed = true
		tg.lifecycleMu.Unlock()
		close(tg.closeCh)
		tg.sendMu.Unlock()
	})
}

func (tg *Target) markDone() {
	tg.doneOnce.Do(func() {
		close(tg.doneCh)
	})
}

// initProducer creates the Kafka producer client with retries.
func (tg *Target) initProducer(ctx context.Context, top relay.Topics) (*kgo.Client, error) {
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

	opts = append(opts, kgo.RecordPartitioner(mixedPartitioner{
		automatic: kgo.StickyKeyPartitioner(nil),
	}))

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

	// Attempt the initial connection once, then apply max_retries to failures
	// after that initial attempt. This makes max_retries=0 mean "try once".
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			cl, err = kgo.NewClient(opts...)
			if err != nil {
				tg.metr.networkErrClientCreation.Inc()
				tg.log.Error("error creating producer client", "error", err)
				retries++
				if tg.pCfg.MaxRetries != relay.IndefiniteRetry && retries > tg.pCfg.MaxRetries {
					break
				}
				relay.WaitTries(ctx, backoff(retries))
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
			validationErr := relay.ValidateConn(ctx, cl, tg.pCfg.SessionTimeout, topics, partitions)
			if validationErr != nil {
				cl.Close()

				tg.metr.networkErrConnection.Inc()
				err = validationErr
				tg.log.Error("error connecting to producer", "err", validationErr)
				retries++
				if tg.pCfg.MaxRetries != relay.IndefiniteRetry && retries > tg.pCfg.MaxRetries {
					break
				}
				relay.WaitTries(ctx, backoff(retries))
				continue
			}

			return cl, nil
		}
		break
	}

	if err == nil {
		err = fmt.Errorf("producer connection failed after %d retries", retries)
	}
	return nil, fmt.Errorf("producer connection failed after %d retries: %w", retries, err)
}

// drain drains any remaining buffered messages from inletCh and flushes the batch.
func (tg *Target) drain(ctx context.Context) error {
	now := time.Now()
	for {
		select {
		case rec := <-tg.inletCh:
			tg.batch = append(tg.batch, rec)
			if len(tg.batch) >= tg.pCfg.FlushBatchSize {
				if err := tg.flush(ctx); err != nil {
					return err
				}
			}
		default:
			ct := len(tg.batch)
			if ct > 0 {
				if err := tg.flush(ctx); err != nil {
					return err
				}
			}

			tg.log.Debug("flushing producer batch", "elapsed", time.Since(now).Seconds(), "count", ct)
			tg.batch = tg.batch[:0]
			return nil
		}
	}
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
		lastErr error
	)

retry:
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batchLen := len(tg.batch)
		tg.metr.flushBatchSize.Update(float64(batchLen))

		// Check that the destination cluster is healthy before retrying.
		if retries > 0 && !relay.CheckTCP(ctx, tg.pCfg.BootstrapBrokers, tg.pCfg.SessionTimeout) {
			retries++
			tg.metr.flushRetries.Inc()
			if tg.pCfg.MaxRetries != relay.IndefiniteRetry && retries > tg.pCfg.MaxRetries {
				break retry
			}
			backoff := backOff(retries)
			tg.log.Debug("destination unavailable; waiting before retry", "backoff", backoff.Seconds(), "retry", retries)
			relay.WaitTries(ctx, backoff)
			continue
		}

		tg.log.Debug("producing message", "broker", tg.client.OptValue(kgo.SeedBrokers), "msgs", batchLen, "retry", retries)
		results := tg.client.ProduceSync(ctx, tg.batch...)

		var (
			err      error
			failures []*kgo.Record
		)
		for _, res := range results {
			if res.Err == context.Canceled {
				return ctx.Err()
			}

			if res.Err != nil {
				// Reset context/timestamp so the record can be produced again.
				rec := res.Record
				rec.Timestamp = time.Time{}
				rec.Context = nil

				failures = append(failures, rec)
				err = res.Err
				continue
			}
		}

		tg.log.Debug("produced last offset", "offset", results[len(results)-1].Record.Offset, "batch", batchLen, "retry", retries)

		if err != nil {
			lastErr = err
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
			if tg.pCfg.MaxRetries != relay.IndefiniteRetry && retries > tg.pCfg.MaxRetries {
				break retry
			}

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
		if lastErr != nil {
			return fmt.Errorf("error producing message; exhausted retries (%v): %w", tg.pCfg.MaxRetries, lastErr)
		}
		return fmt.Errorf("error producing message; exhausted retries (%v)", tg.pCfg.MaxRetries)
	}

	tg.metr.flushDuration.UpdateDuration(start)
	return nil
}
