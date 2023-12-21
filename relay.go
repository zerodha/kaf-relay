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
	if err := r.validateOffsets(ctx); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	// track topic lag and switch over the client if needed.
	if r.lagThreshold > 0 {
		wg.Add(1)
		go r.trackTopicLag(ctx, wg)
	}

	// Flush the producer after the poll loop is over.
	defer func() {
		if err := r.producer.client.Flush(ctx); err != nil {
			r.logger.Error("error while flushing the producer", "err", err)
		}
	}()

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
					r.logger.Debug("reached end of offsets; stopping relay", "broker", cl.OptValue(kgo.SeedBrokers), "offsets", r.endOffsets)
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

					r.consumerMgr.setTopicOffsets(rec)
				})
			}
		}
	}

	wg.Wait()

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
					setup := func() {
						atomic.CompareAndSwapUint32(&r.consumerMgr.reconnectInProgress, StateDisconnected, StateConnecting)
						r.consumerMgr.Lock()
					}
					cleanup := func() {
						r.consumerMgr.Unlock()
						atomic.CompareAndSwapUint32(&r.consumerMgr.reconnectInProgress, StateConnecting, StateDisconnected)
					}

					setup()
					// get the current polling context and cancel to break the current poll loop
					addrs := r.consumerMgr.getClient(idx).OptValue(kgo.SeedBrokers)
					r.logger.Debug("lag threshold exceeded; switching over", "broker", addrs)

					r.consumerMgr.setActive(idx - 1)
					if err := r.consumerMgr.connectToNextNode(); err != nil {
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

	commitOffsets, err := getCommittedOffsets(ctx, c, consTopics)
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
