package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	RelayMetric = "kafka_relay_msg_count{source=\"%s\", destination=\"%s\", partition=\"%d\"}"
)

// relay represents the input, output kafka and the remapping necessary to forward messages from one topic to another.
type relay struct {
	consumerMgr *consumerManager
	producer    *producer

	topics  map[string]string
	metrics *metrics.Set
	logger  *slog.Logger

	// retry
	retryBackoffFn func(int) time.Duration
	maxRetries     int
	retries        int
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async producer.
func (r *relay) Start(ctx context.Context) error {
	if err := r.validateOffsets(ctx); err != nil {
		return err
	}

pollLoop:
	for {
		// exit if max retries is exhausted
		if r.retries >= r.maxRetries {
			return fmt.Errorf("`max_retries`(%d) exhausted; exiting relay", r.maxRetries)
		}

		select {
		case <-ctx.Done():
			break pollLoop
		default:
			// get consumer specific context, client
			r.consumerMgr.Lock()
			childCtx, _ := r.consumerMgr.getCurrentContext()
			cl := r.consumerMgr.getCurrentClient()
			r.consumerMgr.Unlock()

			r.logger.Debug("polling active consumer", "broker", cl.OptValue(kgo.SeedBrokers))
			fetches := cl.PollFetches(childCtx)

			if fetches.IsClientClosed() {
				r.retries++
				waitTries(ctx, r.retryBackoffFn(r.retries))

				r.logger.Debug("consumer group fetch client closed")
				continue pollLoop
			}

			// Proxy fetch errors into error callback function
			errs := fetches.Errors()
			for _, err := range errs {
				if errors.Is(err.Err, kgo.ErrClientClosed) {
					r.retries++
					waitTries(ctx, r.retryBackoffFn(r.retries))

					r.logger.Debug("consumer group fetch error client closed", "broker", cl.OptValue(kgo.SeedBrokers))
					continue pollLoop
				}

				if errors.Is(err.Err, context.Canceled) {
					r.retries++
					waitTries(ctx, r.retryBackoffFn(r.retries))

					r.logger.Debug("consumer group fetch error client context cancelled", "broker", cl.OptValue(kgo.SeedBrokers))
					continue pollLoop
				}

				r.retries++
				waitTries(ctx, r.retryBackoffFn(r.retries))
				r.logger.Error("error while consuming", "err", err.Err)
			}

			// reset max retries if successfull
			r.retries = 0

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()

				// Fetch destination topic. Ignore if remapping is not defined.
				t, ok := r.topics[rec.Topic]
				if !ok {
					continue
				}

				msg := &kgo.Record{
					Key:       rec.Key,
					Value:     rec.Value,
					Topic:     t, // remap destination topic
					Partition: rec.Partition,
				}

				r.metrics.GetOrCreateCounter(fmt.Sprintf(RelayMetric, rec.Topic, t, rec.Partition)).Inc()

				r.logger.Debug("producing message", "broker", r.producer.client.OptValue(kgo.SeedBrokers), "msg", msg)

				// Async producer which internally batches the messages as per producer `batch_size`
				r.producer.client.Produce(ctx, msg, func(rec *kgo.Record, err error) {
					if err != nil {
						r.logger.Error("error producing message", "err", err)
						return
					}

					// Mark / commit offsets
					r.consumerMgr.commit(rec)
				})
			}
		}
	}

	return nil
}

// getMetrics writes the internal prom metrics to the given io.Writer
func (r *relay) getMetrics(buf io.Writer) {
	r.metrics.WritePrometheus(buf)
}

// validateOffsets makes sure that source, destination has same topic, partitions.
func (r *relay) validateOffsets(ctx context.Context) error {
	var (
		consTopics []string
		prodTopics []string
	)
	for c, p := range r.topics {
		consTopics = append(consTopics, c)
		prodTopics = append(prodTopics, p)
	}

	r.consumerMgr.Lock()
	c := r.consumerMgr.getCurrentClient()
	r.consumerMgr.Unlock()

	consOffsets, err := getEndOffsets(ctx, c, consTopics)
	if err != nil {
		return err
	}
	prodOffsets, err := getEndOffsets(ctx, r.producer.client, prodTopics)
	if err != nil {
		return err
	}

	commitOffsets, err := getCommittedOffsets(ctx, c, consTopics)
	if err != nil {
		return err
	}

	for _, ps := range consOffsets {
		for _, o := range ps {
			// Check if mapping exists
			t, ok := r.topics[o.Topic]
			if !ok {
				return fmt.Errorf("error finding destination topic for %v in given mapping", o.Topic)
			}

			// Check if topic, partition exists in destination
			destOffset, ok := prodOffsets.Lookup(t, o.Partition)
			if !ok {
				return fmt.Errorf("error finding destination topic, partition for %v in destination kafka", o.Topic)
			}

			// Confirm that committed offsets of consumer group matches the offsets of destination kafka topic partition
			if destOffset.Offset > 0 {
				o, ok := commitOffsets.Lookup(o.Topic, destOffset.Partition)
				if !ok {
					return fmt.Errorf("error finding topic, partition for %v in source kafka", o.Topic)
				}

				if destOffset.Offset > o.Offset {
					return fmt.Errorf("destination topic(%v), partition(%v) offsets(%v) is higher than consumer group committed offsets(%v)",
						destOffset.Topic, destOffset.Partition, destOffset.Offset, o.Offset)
				}
			}
		}
	}

	return nil
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
