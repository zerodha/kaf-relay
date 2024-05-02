package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TargetCfg is the producer/target Kafka config.
type TargetCfg struct {
	ReqTimeout    time.Duration
	EnableBackoff bool
	BackoffMin    time.Duration
	BackoffMax    time.Duration
}

// Target is a holder for the kafka Target client.
type Target struct {
	client  *kgo.Client
	cfg     TargetCfg
	pCfg    ProducerCfg
	ctx     context.Context
	metrics *metrics.Set
	log     *slog.Logger

	// Map of optional destination topic partitions.
	topicPartitions map[string]int32

	// Map of target topics and their config.
	targetTopics Topics

	batchCh chan *kgo.Record
	batch   []*kgo.Record
}

// NewTarget returns a new producer relay that handles target Kafka instances.
func NewTarget(globalCtx context.Context, cfg TargetCfg, pCfg ProducerCfg, topics Topics, m *metrics.Set, log *slog.Logger) (*Target, error) {
	p := &Target{
		cfg:     cfg,
		pCfg:    pCfg,
		ctx:     globalCtx,
		metrics: m,
		log:     log,

		batch:   make([]*kgo.Record, 0, pCfg.BatchSize),
		batchCh: make(chan *kgo.Record),
	}

	// Initialize the actual Kafka client.
	cl, err := p.initProducer(topics)
	if err != nil {
		return nil, err
	}

	p.client = cl

	return p, nil
}

// Close closes the kafka client.
func (tg *Target) Close() {
	if tg.client != nil {
		// prevent blocking on close
		tg.client.PurgeTopicsFromProducing()
	}
}

// CloseBatchCh closes the Producer batch channel.
func (tg *Target) CloseBatchCh() {
	close(tg.batchCh)
}

// GetBatchCh returns the Producer batch channel.
func (tg *Target) GetBatchCh() chan *kgo.Record {
	return tg.batchCh
}

// prepareRecord checks if custom topic partition mapping is defined.
// If required, it updates the records partition
func (tg *Target) prepareRecord(rec *kgo.Record) {
	if part, ok := tg.topicPartitions[rec.Topic]; ok {
		rec.Partition = part
	}
}

// Start starts the blocking producer which flushes messages to the target Kafka.
func (tg *Target) Start(ctx context.Context) error {
	tick := time.NewTicker(tg.pCfg.FlushFrequency)
	defer tick.Stop()

	// Flush the Producer after the poll loop is over.
	defer func() {
		if err := tg.client.Flush(ctx); err != nil {
			tg.log.Error("error while flushing the producer", "err", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			if err := tg.drain(); err != nil {
				return err
			}

			return ctx.Err()

		// Queue the message to and flush if the batch size is reached.
		case msg, ok := <-tg.batchCh:
			if !ok {
				// Flush and cleanup on exit.
				if err := tg.drain(); err != nil {
					return err
				}

				return nil
			}

			tg.prepareRecord(msg)
			tg.batch = append(tg.batch, msg)
			if len(tg.batch) >= tg.pCfg.FlushBatchSize {
				if err := tg.flush(ctx); err != nil {
					return err
				}

				tick.Reset(tg.pCfg.FlushFrequency)
			}

		// flush the Producer batch at a given frequency.
		case <-tick.C:
			if len(tg.batch) > 0 {
				if err := tg.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// GetHighWatermark returns the offsets on the target topics.
func (tg *Target) GetHighWatermark() (kadm.ListedOffsets, error) {
	var out []string
	for _, t := range tg.targetTopics {
		out = append(out, t.TargetTopic)
	}

	return getHighWatermark(tg.ctx, tg.client, out, tg.cfg.ReqTimeout)
}

// initProducer returns a Kafka producer client.
func (tg *Target) initProducer(top Topics) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.ProduceRequestTimeout(tg.pCfg.SessionTimeout),
		kgo.RecordDeliveryTimeout(tg.pCfg.SessionTimeout), // break the :ProduceSync if it takes too long
		kgo.ProducerBatchMaxBytes(int32(tg.pCfg.MaxMessageBytes)),
		kgo.MaxBufferedRecords(tg.pCfg.FlushBatchSize),
		kgo.ProducerLinger(tg.pCfg.FlushFrequency),
		kgo.ProducerBatchCompression(getCompressionCodec(tg.pCfg.Compression)),
		kgo.SeedBrokers(tg.pCfg.BootstrapBrokers...),
		kgo.RequiredAcks(getAckPolicy(tg.pCfg.CommitAck)),
	}

	// TCPAck/LeaderAck requires Kafka deduplication to be turned off.
	if !tg.pCfg.EnableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	if tg.pCfg.EnableAuth {
		opts = addSASLConfig(opts, tg.pCfg.KafkaCfg)
	}

	if tg.pCfg.EnableTLS {
		if tg.pCfg.CACertPath == "" && tg.pCfg.ClientCertPath == "" && tg.pCfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := getTLSConfig(tg.pCfg.CACertPath, tg.pCfg.ClientCertPath, tg.pCfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	var (
		retries = 0
		backoff = getBackoffFn(tg.cfg.EnableBackoff, tg.cfg.BackoffMin, tg.cfg.BackoffMax)
		err     error
		cl      *kgo.Client
	)

	// Retry until a successful connection.
outerLoop:
	for retries < tg.pCfg.MaxRetries || tg.pCfg.MaxRetries == IndefiniteRetry {
		select {
		case <-tg.ctx.Done():
			break outerLoop
		default:
			cl, err = kgo.NewClient(opts...)
			if err != nil {
				tg.log.Error("error creating producer client", "error", err)
				retries++
				waitTries(tg.ctx, backoff(retries))
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

			// Test connectivity and ensure destination topics exists.
			if err := testConnection(cl, tg.pCfg.SessionTimeout, topics, partitions); err != nil {
				tg.log.Error("error connecting to producer", "err", err)
				retries++
				waitTries(tg.ctx, backoff(retries))
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
	for rec := range tg.batchCh {
		tg.prepareRecord(rec)
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

// flush flushes the Producer batch to kafka and updates the topic offsets.
func (tg *Target) flush(ctx context.Context) error {
	var (
		retries = 0
		sent    bool
		backOff = getBackoffFn(tg.cfg.EnableBackoff, tg.cfg.BackoffMin, tg.cfg.BackoffMax)
	)

retry:
	for retries < tg.pCfg.MaxRetries || tg.pCfg.MaxRetries == IndefiniteRetry {
		batchLen := len(tg.batch)

		// check if the destination cluster is healthy before attempting to produce again.
		if retries > 0 && !checkTCP(ctx, tg.pCfg.BootstrapBrokers, tg.pCfg.SessionTimeout) {
			continue
		}

		tg.log.Debug("producing message", "broker", tg.client.OptValue(kgo.SeedBrokers), "msgs", batchLen, "retry", retries)
		results := tg.client.ProduceSync(ctx, tg.batch...)

		// Check for error and use that to identify what messages failed to produce.
		var (
			err      error
			failures []*kgo.Record
		)
		for idx, res := range results {
			// exit if context is cancelled
			if res.Err == context.Canceled {
				return ctx.Err()
			}

			// Gather the failed messages and retry.
			if res.Err != nil {
				// reset context, timestamp for the given message to be produced again
				// read: kgo.Client{}.Produce() comment
				tg.batch[idx].Timestamp = time.Time{}
				tg.batch[idx].Context = nil

				failures = append(failures, tg.batch[idx])
				err = res.Err
				continue
			}

			var (
				srcTopic  = res.Record.Topic
				destTopic = tg.targetTopics[res.Record.Topic]
				part      = res.Record.Partition
			)
			tg.metrics.GetOrCreateCounter(fmt.Sprintf(RelayMetric, srcTopic, destTopic, part)).Inc()
		}

		tg.log.Debug("produced last offset", "offset", results[len(results)-1].Record.Offset, "batch", batchLen, "retry", retries)

		// retry if there is an error
		if err != nil {
			tg.log.Error("error producing message", "err", err, "failed_count", batchLen, "retry", retries)

			bufRecs := tg.client.BufferedProduceRecords()
			if bufRecs > 0 {
				if err := tg.client.AbortBufferedRecords(ctx); err != nil {
					tg.log.Error("error aborting buffered records", "err", err)
				} else {
					tg.log.Debug("aborted buffered records, retrying failed messages", "recs", bufRecs)
				}
			}

			// reset the batch to the failed messages
			tg.batch = tg.batch[:0]
			tg.batch = failures
			retries++

			// backoff retry
			b := backOff(retries)
			tg.log.Debug("error producing message; waiting for retry...", "backoff", b.Seconds())
			waitTries(ctx, b)

			continue retry
		}

		// reset the batch to the remaining messages
		tg.batch = tg.batch[batchLen:]
		sent = true
		break retry
	}

	if !sent {
		return fmt.Errorf("error producing message; exhausted retries (%v)", tg.pCfg.MaxRetries)
	}

	return nil
}
