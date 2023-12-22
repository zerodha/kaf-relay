package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/joeirimpan/kaf-relay/filter"
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

	producerBatchCh chan *kgo.Record
	producerBatch   []*kgo.Record

	topics  map[string]string
	metrics *metrics.Set
	logger  *slog.Logger

	// retry
	retryBackoffFn func(int) time.Duration
	maxRetries     int
	retries        int

	// Save the end offset of topic; decrement and make it easy to stop at 0
	endOffsets map[string]map[int32]int64
	stopAtEnd  bool

	// monitor lags
	lagThreshold   int64
	lagMonitorFreq time.Duration

	// list of filter implementations for skipping messages
	filters map[string]filter.Provider
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async producer.
func (r *relay) Start(ctx context.Context) error {
	var prodTopics = make([]string, len(r.topics))
	for k := range r.topics {
		prodTopics = append(prodTopics, k)
	}

	prodOffsets, err := getEndOffsets(ctx, r.producer.client, prodTopics)
	if err != nil {
		return err
	}

	r.consumerMgr.setOffsets(prodOffsets.KOffsets())
	if err := r.validateOffsets(ctx); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	// track topic lag and switch over the client if needed.
	if r.lagThreshold > 0 {
		wg.Add(1)
		go r.trackTopicLag(ctx, wg)
	}

	// start producer worker
	wg.Add(1)
	go func() {
		if err := r.startProducerWorker(ctx, wg); err != nil {
			r.logger.Error("error starting producer worker", "err", err)
		}

		// producer worker exited because there is no active upstream; cancel the context to exit the consumer loop.
		if ctx.Err() != context.Canceled {
			cancel()
		}
	}()

	// Flush the producer after the poll loop is over.
	defer func() {
		if err := r.producer.client.Flush(ctx); err != nil {
			r.logger.Error("error while flushing the producer", "err", err)
		}
	}()

	r.startConsumerWorker(ctx)

	wg.Wait()

	return nil
}

// startConsumerWorker starts the consumer worker which polls the kafka cluster for messages.
func (r *relay) startConsumerWorker(ctx context.Context) error {
pollLoop:
	for {
		// exit if max retries is exhausted
		if r.retries >= r.maxRetries && r.maxRetries != IndefiniteRetry {
			return fmt.Errorf("`max_retries`(%d) exhausted; exiting relay", r.maxRetries)
		}

		select {
		case <-ctx.Done():
			break pollLoop
		default:
			// get consumer specific context, client, config
			r.consumerMgr.Lock()
			var (
				childCtx, _ = r.consumerMgr.getCurrentContext()
				cl          = r.consumerMgr.getCurrentClient()
			)
			r.consumerMgr.Unlock()

			// Stop the poll loop if we reached the end of offsets fetched on boot.
			if r.stopAtEnd {
				if hasReachedEnd(r.endOffsets) {
					r.logger.Info("reached end of offsets; stopping relay", "broker", cl.OptValue(kgo.SeedBrokers), "offsets", r.endOffsets)
					break pollLoop
				}
			}

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
					r.logger.Info("consumer group fetch error client closed", "broker", cl.OptValue(kgo.SeedBrokers))
				}

				if errors.Is(err.Err, context.Canceled) {
					r.logger.Info("consumer group fetch error client context cancelled", "broker", cl.OptValue(kgo.SeedBrokers))
				}

				r.retries++
				waitTries(ctx, r.retryBackoffFn(r.retries))
				r.logger.Error("error while consuming", "err", err.Err)

				continue pollLoop
			}

			// reset max retries if successfull
			r.retries = 0

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()

				// Decrement the end offsets for the given topic, partition till we reach 0
				if r.stopAtEnd {
					if ct, ok := r.endOffsets[rec.Topic]; ok {
						o := ct[rec.Partition]
						if o > 0 {
							o -= 1
							ct[rec.Partition] = o
							r.endOffsets[rec.Topic] = ct
						}
					}
				}

				// check if message needs to skipped?
				msgAllowed := true
				for n, f := range r.filters {
					if !f.IsAllowed(rec.Value) {
						r.logger.Debug("filtering message", "message", string(rec.Value), "filter", n)
						msgAllowed = false
						break
					}
				}

				if !msgAllowed {
					continue
				}

				// Fetch destination topic. Ignore if remapping is not defined.
				t, ok := r.topics[rec.Topic]
				if !ok {
					continue
				}

				// queue the message for producing in batch
				select {
				case <-ctx.Done():
					r.logger.Debug("context cancelled; exiting relay")
					close(r.producerBatchCh)
					break pollLoop
				case r.producerBatchCh <- &kgo.Record{
					Key:       rec.Key,
					Value:     rec.Value,
					Topic:     t, // remap destination topic
					Partition: rec.Partition,
				}:
					// default:
					// 	r.logger.Debug("producer batch channel full")
				}
			}
		}
	}

	return nil
}

// startProducerWorker starts producer worker which flushes the producer batch at a given frequency.
func (r *relay) startProducerWorker(ctx context.Context, wg *sync.WaitGroup) error {
	tick := time.NewTicker(r.producer.cfg.FlushFrequency)
	defer tick.Stop()
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			// reset
			r.producerBatch = r.producerBatch[:0]
			return ctx.Err()

		// enqueue the message to the producer batch and flush if batch size is reached.
		case msg, ok := <-r.producerBatchCh:

			// cleanup; close the producer batch channel and flush the remaining messages
			if !ok {
				now := time.Now()
				ct := 0
				// ignore other messages; we can fetch the produced offsets from producer topics and start from there on boot.
				for range r.producerBatchCh {
					ct++
				}

				r.logger.Debug("flushing producer batch", "elapsed", time.Since(now), "count", ct)
				return nil
			}

			r.producerBatch = append(r.producerBatch, msg)

			if len(r.producerBatch) >= r.producer.cfg.FlushBatchSize {
				if err := r.flushProducer(ctx); err != nil {
					return err
				}

				tick.Reset(r.producer.cfg.FlushFrequency)
			}

		// flush the producer batch at a given frequency.
		case <-tick.C:
			if len(r.producerBatch) > 0 {
				if err := r.flushProducer(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// flushProducer flushes the producer batch to kafka and updates the topic offsets.
func (r *relay) flushProducer(ctx context.Context) error {
	var (
		retries = 0
		sent    bool
		backOff = retryBackoff()
	)

	saveOffsetsFor := func(recs []*kgo.Record) {
		// set the topic offset for records that were successfully produced
		r.consumerMgr.Lock()
		for i := 0; i < len(recs); i++ {
			r.consumerMgr.setTopicOffsets(recs[i])
		}
		r.consumerMgr.Unlock()
	}

retry:
	for retries < r.producer.cfg.MaxRetries {
		batchLen := len(r.producerBatch)

		r.logger.Info("producing message", "broker", r.producer.client.OptValue(kgo.SeedBrokers), "msgs", batchLen, "retry", retries)
		results := r.producer.client.ProduceSync(ctx, r.producerBatch...)

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
				r.logger.Error("error producing message results", "err", res.Err)
				err = res.Err
				continue
			}

			successes = append(successes, res.Record)
			var (
				srcTopic  = res.Record.Topic
				destTopic = r.topics[res.Record.Topic]
				p         = res.Record.Partition
			)
			r.metrics.GetOrCreateCounter(fmt.Sprintf(RelayMetric, srcTopic, destTopic, p)).Inc()
		}

		// retry if there is an error
		if err != nil {
			r.logger.Error("error producing message", "err", err, "failed_count", batchLen, "retry", retries)

			// save offsets for all successfull messages
			saveOffsetsFor(successes)

			// reset the batch to the failed messages
			r.producerBatch = failures
			retries++

			// backoff retry
			b := backOff(retries)
			r.logger.Debug("error producing message; waiting for retry...", "backoff", b.Seconds())
			waitTries(ctx, b)

			continue retry
		}

		// save offsets for all successfull messages
		saveOffsetsFor(successes)

		// reset the batch to the remaining messages
		r.producerBatch = r.producerBatch[batchLen:]
		sent = true
		break retry
	}

	if !sent {
		return fmt.Errorf("error producing message; exhausted retries (%v)", r.producer.cfg.MaxRetries)
	}

	return nil
}

// getMetrics writes the internal prom metrics to the given io.Writer
func (r *relay) getMetrics(buf io.Writer) {
	r.metrics.WritePrometheus(buf)
}

// trackTopicLag tracks the topic lag between servers and switches over if threshold breached.
func (r *relay) trackTopicLag(ctx context.Context, wg *sync.WaitGroup) {
	tick := time.NewTicker(r.lagMonitorFreq)
	defer tick.Stop()
	defer wg.Done()

	// XXX: Create separate clients for tracking offsets to prevent locking
	// main consumer manager.
	// create a copy of the consumer configs
	r.consumerMgr.Lock()
	cfgs := make([]ConsumerGroupCfg, len(r.consumerMgr.c.cfgs))
	copy(cfgs, r.consumerMgr.c.cfgs)
	r.consumerMgr.Unlock()

	// create admin clients for these configs
	clients := make([]*kadm.Client, len(cfgs))
	for i, cfg := range cfgs {
		cl, err := getClient(cfg)
		if err != nil {
			r.logger.Error("error creating admin client for tracking lag", "err", err)
			continue
		}
		clients[i] = kadm.NewClient(cl)
	}

loop:
	for {
		select {
		case <-ctx.Done():
			break loop

		case <-tick.C:
			r.logger.Debug("checking topic lag", "freq", r.lagMonitorFreq.Seconds())

			r.consumerMgr.Lock()
			curr := r.consumerMgr.Index()
			r.consumerMgr.Unlock()
			var (
				n           = len(cfgs)
				currOffsets kadm.ListedOffsets
			)
		lagCheck:
			for i := 0; i < n; i++ {
				idx := (curr + i) % n // wraparound
				cl := clients[idx]
				if cl == nil {
					continue
				}
				cfg := cfgs[idx]

				up := false
				for _, addr := range cfg.BootstrapBrokers {
					if conn, err := net.DialTimeout("tcp", addr, time.Second); err != nil && checkErr(err) {
						up = false
					} else {
						conn.Close()
						up = true
						break
					}
				}

				// skip pulling offsets if the nodes are not up
				if !up {
					continue
				}

				of, err := cl.ListEndOffsets(ctx, cfg.Topics...)
				if err != nil {
					r.logger.Error("error fetching end offsets; tracking topic lag", "err", err)
					continue lagCheck
				}

				if idx == curr {
					currOffsets = of
					continue lagCheck
				}

				if currOffsets != nil && thresholdExceeded(of, currOffsets, r.lagThreshold) {
					// setup method locks consumer manager
					setup := func() {
						atomic.CompareAndSwapUint32(&r.consumerMgr.reconnectInProgress, StateDisconnected, StateConnecting)
						r.consumerMgr.Lock()
					}
					// cleanup method unlocks the consumer manager
					cleanup := func() {
						r.consumerMgr.Unlock()
						atomic.CompareAndSwapUint32(&r.consumerMgr.reconnectInProgress, StateConnecting, StateDisconnected)
					}

					setup()
					// get the current polling context and cancel to break the current poll loop
					addrs := r.consumerMgr.getClient(idx).OptValue(kgo.SeedBrokers)
					r.logger.Info("lag threshold exceeded; switching over", "broker", addrs)

					// set index to 1 less than the index we are going to increment inside `connectToNextNode`
					r.consumerMgr.setActive(idx - 1)
					if err := r.consumerMgr.connectToNextNode(); err != nil {
						// reset the index back to original on error
						r.consumerMgr.setActive(curr)
						cleanup()
						continue lagCheck
					}

					cancelFn := r.consumerMgr.getCancelFn(curr)
					cancelFn()

					cleanup()
					break lagCheck
				}
			}
		}
	}
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

	c := r.consumerMgr.getCurrentClient()
	consOffsets, err := getEndOffsets(ctx, c, consTopics)
	if err != nil {
		return err
	}

	prodOffsets, err := getEndOffsets(ctx, r.producer.client, prodTopics)
	if err != nil {
		return err
	}

	for _, ps := range consOffsets {
		for _, o := range ps {
			// store the end offsets
			if r.stopAtEnd {
				ct, ok := r.endOffsets[o.Topic]
				if !ok {
					ct = make(map[int32]int64)
				}
				ct[o.Partition] = o.Offset
				r.endOffsets[o.Topic] = ct
			}

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
			if destOffset.Offset > o.Offset {
				return fmt.Errorf("destination topic(%v), partition(%v) offsets(%v) is higher than consumer group committed offsets(%v)",
					destOffset.Topic, destOffset.Partition, destOffset.Offset, o.Offset)
			}
		}
	}

	return nil
}
