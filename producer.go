package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// producer is a holder for the kafka producer client.
type producer struct {
	client     *kgo.Client
	cfg        ProducerCfg
	backoffCfg BackoffCfg
	maxReqTime time.Duration

	batchCh chan *kgo.Record
	batch   []*kgo.Record

	logger  *slog.Logger
	metrics *metrics.Set
}

// Close closes the kafka client.
func (p *producer) Close() {
	if p.client != nil {
		// prevent blocking on close
		p.client.PurgeTopicsFromProducing()
	}
}

// CloseBatchCh closes the Producer batch channel.
func (p *producer) CloseBatchCh() {
	close(p.batchCh)
}

// GetBatchCh returns the Producer batch channel.
func (p *producer) GetBatchCh() chan *kgo.Record {
	return p.batchCh
}

// prepareRecord checks if custom topic partition mapping is defined.
// If required, it updates the records partition
func (p *producer) prepareRecord(rec *kgo.Record) {
	if p.cfg.TopicsPartition == nil {
		return
	}

	part, ok := p.cfg.TopicsPartition[rec.Topic]
	if ok {
		rec.Partition = part
	}
}

// startProducerWorker starts Producer worker which flushes the Producer batch at a given frequency.
func (p *producer) StartWorker(ctx context.Context) error {
	tick := time.NewTicker(p.cfg.FlushFrequency)
	defer tick.Stop()

	// Flush the Producer after the poll loop is over.
	defer func() {
		if err := p.client.Flush(ctx); err != nil {
			p.logger.Error("error while flushing the producer", "err", err)
		}
	}()

	// drain drains the producer channel and buffer on exit.
	drain := func() error {
		now := time.Now()
		// drain the batch
		for rec := range p.batchCh {
			p.prepareRecord(rec)
			p.batch = append(p.batch, rec)
		}

		ct := len(p.batch)
		// flush Producer on exit
		if ct > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), p.cfg.SessionTimeout)
			defer cancel()

			if err := p.flush(ctx); err != nil {
				return err
			}
		}

		p.logger.Debug("flushing producer batch", "elapsed", time.Since(now).Seconds(), "count", ct)
		// reset
		p.batch = p.batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			if err := drain(); err != nil {
				return err
			}

			return ctx.Err()

		// enqueue the message to the Producer batch and flush if batch size is reached.
		case msg, ok := <-p.batchCh:
			// cleanup; close the Producer batch channel and flush the remaining messages
			if !ok {
				if err := drain(); err != nil {
					return err
				}

				return nil
			}

			p.prepareRecord(msg)
			p.batch = append(p.batch, msg)
			if len(p.batch) >= p.cfg.FlushBatchSize {
				if err := p.flush(ctx); err != nil {
					return err
				}

				tick.Reset(p.cfg.FlushFrequency)
			}

		// flush the Producer batch at a given frequency.
		case <-tick.C:
			if len(p.batch) > 0 {
				if err := p.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// flush flushes the Producer batch to kafka and updates the topic offsets.
func (p *producer) flush(ctx context.Context) error {
	var (
		retries = 0
		sent    bool
		backOff = getBackoffFn(p.backoffCfg)
	)

retry:
	for retries < p.cfg.MaxRetries || p.cfg.MaxRetries == IndefiniteRetry {
		batchLen := len(p.batch)

		// check if the destination cluster is healthy before attempting to produce again.
		if retries > 0 && !healthcheck(ctx, p.cfg.BootstrapBrokers, p.maxReqTime) {
			continue
		}

		p.logger.Debug("producing message", "broker", p.client.OptValue(kgo.SeedBrokers), "msgs", batchLen, "retry", retries)
		results := p.client.ProduceSync(ctx, p.batch...)

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
				p.batch[idx].Timestamp = time.Time{}
				p.batch[idx].Context = nil

				failures = append(failures, p.batch[idx])
				err = res.Err
				continue
			}

			var (
				srcTopic  = res.Record.Topic
				destTopic = p.cfg.Topics[res.Record.Topic]
				part      = res.Record.Partition
			)
			p.metrics.GetOrCreateCounter(fmt.Sprintf(RelayMetric, srcTopic, destTopic, part)).Inc()
		}

		p.logger.Debug("produced last offset", "offset", results[len(results)-1].Record.Offset, "batch", batchLen, "retry", retries)

		// retry if there is an error
		if err != nil {
			p.logger.Error("error producing message", "err", err, "failed_count", batchLen, "retry", retries)

			bufRecs := p.client.BufferedProduceRecords()
			if bufRecs > 0 {
				if err := p.client.AbortBufferedRecords(ctx); err != nil {
					p.logger.Error("error aborting buffered records", "err", err)
				} else {
					p.logger.Debug("aborted buffered records, retrying failed messages", "recs", bufRecs)
				}
			}

			// reset the batch to the failed messages
			p.batch = p.batch[:0]
			p.batch = failures
			retries++

			// backoff retry
			b := backOff(retries)
			p.logger.Debug("error producing message; waiting for retry...", "backoff", b.Seconds())
			waitTries(ctx, b)

			continue retry
		}

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

// getOffsetsForConsumer returns the end offsets for the given topics.
func (p *producer) getOffsetsForConsumer(ctx context.Context, timeout time.Duration) (kadm.ListedOffsets, error) {
	var (
		topics         []string
		reverseMapping = make(map[string]string, len(p.cfg.Topics))
	)
	for c, p := range p.cfg.Topics {
		topics = append(topics, p)
		reverseMapping[p] = c
	}

	offsets, err := getEndOffsets(ctx, p.client, topics, timeout)
	if err != nil {
		return nil, err
	}

	// remap destination topic to the consumer topics
	for t, ps := range offsets {
		for i, o := range ps {
			consTopic, ok := reverseMapping[o.Topic]
			if !ok {
				continue
			}

			o.Topic = consTopic
			ps[i] = o
		}
		offsets[t] = ps
	}

	return offsets, nil
}
