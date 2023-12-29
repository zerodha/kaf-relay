package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// producer is a holder for the kafka producer client.
type producer struct {
	client *kgo.Client
	cfg    ProducerCfg

	batchCh chan *kgo.Record
	batch   []*kgo.Record

	offsetsCommitFn func([]*kgo.Record)

	logger  *slog.Logger
	metrics *metrics.Set
}

// setOffsetsCommitFn sets the offsets commit function.
func (p *producer) setOffsetsCommitFn(fn func([]*kgo.Record)) {
	p.offsetsCommitFn = fn
}

// startProducerWorker starts producer worker which flushes the producer batch at a given frequency.
func (p *producer) startWorker(ctx context.Context) error {
	tick := time.NewTicker(p.cfg.FlushFrequency)
	defer tick.Stop()

	// Flush the producer after the poll loop is over.
	defer func() {
		if err := p.client.Flush(ctx); err != nil {
			p.logger.Error("error while flushing the producer", "err", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// drain the batch
			for rec := range p.batchCh {
				p.batch = append(p.batch, rec)
			}

			// flush producer on exit
			if len(p.batch) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), p.cfg.SessionTimeout)
				defer cancel()

				if err := p.flush(ctx); err != nil {
					return err
				}
			}

			// reset
			p.batch = p.batch[:0]
			return ctx.Err()

		// enqueue the message to the producer batch and flush if batch size is reached.
		case msg, ok := <-p.batchCh:

			// cleanup; close the producer batch channel and flush the remaining messages
			if !ok {
				now := time.Now()
				ct := 0
				// ignore other messages; we can fetch the produced offsets from producer topics and start from there on boot.
				for range p.batchCh {
					ct++
				}

				p.logger.Debug("flushing producer batch", "elapsed", time.Since(now), "count", ct)
				return nil
			}

			p.batch = append(p.batch, msg)

			if len(p.batch) >= p.cfg.FlushBatchSize {
				if err := p.flush(ctx); err != nil {
					return err
				}

				tick.Reset(p.cfg.FlushFrequency)
			}

		// flush the producer batch at a given frequency.
		case <-tick.C:
			if len(p.batch) > 0 {
				if err := p.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// flush flushes the producer batch to kafka and updates the topic offsets.
func (p *producer) flush(ctx context.Context) error {
	var (
		retries = 0
		sent    bool
		backOff = retryBackoff()
	)

retry:
	for retries < p.cfg.MaxRetries || p.cfg.MaxRetries == IndefiniteRetry {
		batchLen := len(p.batch)

		p.logger.Info("producing message", "broker", p.client.OptValue(kgo.SeedBrokers), "msgs", batchLen, "retry", retries)
		results := p.client.ProduceSync(ctx, p.batch...)

		// Check for error and use that to identify what messages failed to produce.
		var (
			err       error
			failures  []*kgo.Record
			successes []*kgo.Record
		)
		for _, res := range results {
			// exit if context is cancelled
			if res.Err == context.Canceled {
				return ctx.Err()
			}

			if res.Err != nil {
				failures = append(failures, res.Record)
				p.logger.Error("error producing message results", "err", res.Err)
				err = res.Err
				continue
			}

			successes = append(successes, res.Record)
			var (
				srcTopic  = res.Record.Topic
				destTopic = p.cfg.Topics[res.Record.Topic]
				part      = res.Record.Partition
			)
			p.metrics.GetOrCreateCounter(fmt.Sprintf(RelayMetric, srcTopic, destTopic, part)).Inc()
		}

		// retry if there is an error
		if err != nil {
			p.logger.Error("error producing message", "err", err, "failed_count", batchLen, "retry", retries)

			// save offsets for all successfull messages
			p.offsetsCommitFn(successes)

			// reset the batch to the failed messages
			p.batch = failures
			retries++

			// backoff retry
			b := backOff(retries)
			p.logger.Debug("error producing message; waiting for retry...", "backoff", b.Seconds())
			waitTries(ctx, b)

			continue retry
		}

		// save offsets for all successfull messages
		p.offsetsCommitFn(successes)

		// reset the batch to the remaining messages
		p.batch = p.batch[batchLen:]
		sent = true
		break retry
	}

	if !sent {
		return fmt.Errorf("error producing message; exhausted retries (%v)", p.cfg.MaxRetries)
	}

	return nil
}

// initProducer initializes the kafka producer client.
func initProducer(ctx context.Context, cfg ProducerCfg, l *slog.Logger) (*producer, error) {
	l.Info("creating producer", "broker", cfg.BootstrapBrokers)

	prod := &producer{logger: l}
	opts := []kgo.Opt{
		kgo.AllowAutoTopicCreation(),
		kgo.ProduceRequestTimeout(cfg.SessionTimeout),
		kgo.RecordDeliveryTimeout(cfg.SessionTimeout), // break the :ProduceSync if it takes too long
		kgo.RequestRetries(cfg.MaxRetries),
		kgo.ProducerBatchMaxBytes(int32(cfg.MaxMessageBytes)),
		kgo.MaxBufferedRecords(cfg.BatchSize),
		kgo.ProducerLinger(cfg.FlushFrequency),
		kgo.ProducerBatchCompression(getCompressionCodec(cfg.Compression)),
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.RequiredAcks(getAckPolicy(cfg.CommitAck)),
	}

	// TCPAck/LeaderAck requires kafka deduplication to be turned off
	if !cfg.EnableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	// Add authentication
	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := createTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	var (
		retries = 0
		backoff = retryBackoff()
		err     error
	)

	// retry until we can connect to kafka
outerLoop:
	for retries < cfg.MaxRetries || cfg.MaxRetries == IndefiniteRetry {
		select {
		case <-ctx.Done():
			break outerLoop
		default:
			prod.client, err = kgo.NewClient(opts...)
			if err != nil {
				l.Error("error creating producer client", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			// Get the destination topics
			var topics []string
			for _, v := range cfg.Topics {
				topics = append(topics, v)
			}

			// test connectivity and ensures destination topics exists.
			err = testConnection(prod.client, cfg.SessionTimeout, topics)
			if err != nil {
				l.Error("error connecting to producer", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			if err == nil {
				prod.cfg = cfg
				break outerLoop
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return prod, nil
}
