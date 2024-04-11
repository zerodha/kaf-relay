package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/filter"
)

// Relay represents the input, output kafka and the remapping necessary to forward messages from one topic to another.
type Relay struct {
	consumer *consumer
	producer *producer

	unhealthyCh chan struct{}

	topics  map[string]string
	metrics *metrics.Set
	logger  *slog.Logger

	// retry
	backoffCfg BackoffCfg
	maxRetries int
	retries    int

	// monitor lags
	lagThreshold        int64
	nodeHealthCheckFreq time.Duration

	// max time spent checking node health
	maxReqTime time.Duration

	// Save the end offset of topic; decrement and make it easy to stop at 0
	destOffsets map[string]map[int32]kgo.Offset
	srcOffsets  map[string]map[int32]int64
	stopAtEnd   bool

	// list of filter implementations for skipping messages
	filters map[string]filter.Provider

	// signal to cancel the poll loop context
	// use this to track the current node id in track topic lag
	nodeCh     chan int
	pollCancel context.CancelFunc
}

// getMetrics writes the internal prom metrics to the given io.Writer
func (r *Relay) getMetrics(buf io.Writer) {
	r.metrics.WritePrometheus(buf)
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async
func (r *Relay) Start(ctx context.Context) error {
	// wait for all goroutines to exit
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// create a new context for the consumer loop
	// if consumer loop exits(stop-at-end / parent context cancellation), close the channel and cancel the context to exit the producer loop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// track topic lag and switch over the client if needed.
	r.logger.Info("tracking healthy nodes")
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.trackHealthy(ctx); err != nil {
			r.logger.Error("error tracking healthy nodes", "err", err)
		}
	}()

	// start producer worker
	wg.Add(1)
	r.logger.Info("starting producer worker")
	go func() {
		defer wg.Done()
		if err := r.producer.StartWorker(ctx); err != nil {
			r.logger.Error("error starting producer worker", "err", err)
		}

		// producer worker exited because there is no active upstream; cancel the context to exit the consumer loop.
		if ctx.Err() != context.Canceled {
			cancel()
		}
	}()

	// Start consumer group
	r.logger.Info("starting consumer worker")
	// Trigger consumer to fetch initial healthy node
	r.unhealthyCh <- struct{}{}
	if err := r.startConsumerWorker(ctx); err != nil {
		r.logger.Error("error starting consumer worker", "err", err)
	}

	// close the producer batch channel to drain producer worker on exit
	r.producer.CloseBatchCh()

	wg.Wait()

	return nil
}

// Close close the underlying kgo.Client(s)
func (r *Relay) Close() {
	r.logger.Debug("closing relay consumer, producer...")
	r.consumer.Close()
	r.producer.Close()
}

// startConsumerWorker starts the consumer worker which polls the kafka cluster for messages.
func (r *Relay) startConsumerWorker(ctx context.Context) error {
	backoff := getBackoffFn(r.backoffCfg)

	pollCtx, pollCancelFn := context.WithCancel(context.Background())
	defer pollCancelFn()

	r.pollCancel = pollCancelFn

	var (
		nodeID    = -1
		firstPoll = true
	)
pollLoop:
	for {
		// exit if max retries is exhausted
		if r.retries >= r.maxRetries && r.maxRetries != IndefiniteRetry {
			return fmt.Errorf("`max_retries`(%d) exhausted; exiting relay", r.maxRetries)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.unhealthyCh:
			r.logger.Info("poll loop received unhealthy signal")
			pollCancelFn()

			var (
				succ = false
				err  error
			)

			r.logger.Info("poll loop requesting healthy node")
			for !succ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				nodeID, err = r.consumer.GetHealthy(ctx)
				if err != nil {
					r.logger.Debug("poll loop could not get healthy node", "err", err)
					r.retries++
					waitTries(ctx, backoff(r.retries))
					continue
				}

				err = r.consumer.Connect(ctx, r.consumer.cfgs[nodeID])
				if err != nil {
					r.logger.Debug("poll loop could not re-init consumer", "err", err)
					r.retries++
					waitTries(ctx, backoff(r.retries))
					continue
				}

				r.logger.Info("poll loop got healthy node", "id", nodeID)
				succ = true
			}

			pollCtx, pollCancelFn = context.WithCancel(ctx)
			r.pollCancel = pollCancelFn
			defer pollCancelFn()

		default:
			cl := r.consumer.client

			// Stop the poll loop if we reached the end of offsets fetched on boot.
			if r.stopAtEnd {
				if firstPoll {
					of, err := getEndOffsets(ctx, cl, r.consumer.cfgs[nodeID].Topics, r.maxReqTime)
					if err != nil {
						r.logger.Error("could not get end offsets (first poll); sending unhealthy signal")
						r.unhealthyCh <- struct{}{}

						continue pollLoop
					}

					r.setSourceOffsets(of)
					firstPoll = false
				}

				if hasReachedEnd(r.srcOffsets) {
					r.logger.Info("reached end of offsets; stopping relay", "broker", cl.OptValue(kgo.SeedBrokers), "offsets", r.srcOffsets)
					return nil
				}
			}

			r.logger.Debug("polling active consumer", "broker", cl.OptValue(kgo.SeedBrokers))
			fetches := cl.PollFetches(pollCtx)

			if fetches.IsClientClosed() {
				r.retries++
				r.logger.Error("client closed; sending unhealthy signal")
				if ok := r.consumer.nodeTracker.PushDown(nodeID); ok {
					r.logger.Debug("updated weight", "node", nodeID, "weight", -1)
				}
				r.unhealthyCh <- struct{}{}

				continue pollLoop
			}

			// Proxy fetch errors into error callback function
			errs := fetches.Errors()
			for _, err := range errs {
				r.retries++
				waitTries(ctx, backoff(r.retries))
				r.logger.Error("fetch errors; sending unhealthy signal", "err", err.Err)
				if ok := r.consumer.nodeTracker.PushDown(nodeID); ok {
					r.logger.Debug("updated weight", "node", nodeID, "weight", -1)
				}

				r.unhealthyCh <- struct{}{}

				continue pollLoop
			}

			// reset max retries if successfull
			r.retries = 0

			// iter through messages
			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				// record the offsets before producing because producer retry will guarantee that the record will get produced
				r.consumer.RecordOffsets(rec)

				if err := r.processMessage(ctx, rec); err != nil {
					r.logger.Error("error processing message", "err", err)
					return err
				}
			}

			cl.AllowRebalance()
		}
	}
}

// processMessage processes the given message and forwards it to the producer batch channel.
func (r *Relay) processMessage(ctx context.Context, rec *kgo.Record) error {
	// Decrement the end offsets for the given topic and partition till we reach 0
	if r.stopAtEnd {
		r.decrementSourceOffset(rec.Topic, rec.Partition)
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

	// skip message
	if !msgAllowed {
		return nil
	}

	// Fetch destination topic. Ignore if remapping is not defined.
	t, ok := r.topics[rec.Topic]
	if !ok {
		return nil
	}

	// queue the message for producing in batch
	select {
	case <-ctx.Done():
		return ctx.Err()

	case r.producer.GetBatchCh() <- &kgo.Record{
		Key:       rec.Key,
		Value:     rec.Value,
		Topic:     t, // remap destination topic
		Partition: rec.Partition,
	}:

		// default:
		// 	r.logger.Debug("producer batch channel full")
	}

	return nil
}

// trackHealthy tracks the healthy node in the cluster and checks if the lag threshold is exceeded.
func (r *Relay) trackHealthy(ctx context.Context) error {
	tick := time.NewTicker(r.nodeHealthCheckFreq)
	defer tick.Stop()

	// XXX: Create separate clients for tracking offsets to prevent locking
	// main consumer manager.
	// create a copy of the consumer configs
	cfgs := make([]ConsumerGroupCfg, len(r.consumer.cfgs))
	copy(cfgs, r.consumer.cfgs)

	// create admin clients for these configs
	clients := make([]*kgo.Client, len(cfgs))
	for i, cfg := range cfgs {
		cl, err := getClient(cfg)
		if err != nil {
			r.logger.Error("error creating admin client for tracking lag", "err", err)
			continue
		}
		clients[i] = cl
	}

	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("closing node tracker go-routine")
			// close all admin clients
			for _, cl := range clients {
				if cl != nil {
					cl.Close()
				}
			}

			return ctx.Err()

		case <-tick.C:
			r.logger.Debug("checking topic lag", "freq", r.nodeHealthCheckFreq.Seconds())
			var (
				n  = len(cfgs)
				cl *kgo.Client
			)

			// Fill metadata for each node
			wg := &sync.WaitGroup{}
			metadata := make([]*NodeMeta, n)
			for i := 0; i < n; i++ {
				r.logger.Debug("tracker checking node", "id", i)

				node := &NodeMeta{ID: i}
				metadata[i] = node

				// create a new client if it doesn't exist
				if clients[i] == nil {
					var err error
					cl, err = getClient(cfgs[i])
					if err != nil {
						r.logger.Error("error creating admin client for tracking lag", "err", err)
						continue
					}

					clients[i] = cl
				}

				// populate offsets, whether the node is healthy in nodemeta.
				wg.Add(1)
				go func(index int) {
					populateMetadata(ctx, clients[index], cfgs[index], node, r.maxReqTime, r.logger)
					wg.Done()
				}(i)
			}
			wg.Wait()

			// find the current node and rearrange the nodes in node tracker depending on healthiness
			currentIndex := -1
			for i := 0; i < n; i++ {
				nodeMeta := metadata[i]
				r.logger.Debug("tracker updating node", "id", i, "data", nodeMeta)

				if !nodeMeta.Healthy {
					r.logger.Debug("pushing down", "id", i)
					if ok := r.consumer.nodeTracker.PushDown(i); ok {
						r.logger.Debug("updated weight", "node", i, "weight", -1)
					}
					continue
				}

				if nodeMeta.IsCurrent {
					currentIndex = i
				}

				var weight int
				nodeMeta.Offsets.Each(func(lo kadm.ListedOffset) {
					weight += int(lo.Offset)
				})

				r.logger.Debug("pushing up", "id", i, "weight", weight)
				if ok := r.consumer.nodeTracker.PushUp(i, weight); ok {
					r.logger.Debug("updated weight", "node", i, "weight", weight)
				}
			}

			// could not find current node; check in next tick
			if currentIndex == -1 {
				continue
			}

			for i := 0; i < n; i++ {
				if i == currentIndex || !metadata[i].Healthy {
					continue
				}

				// check if threshold is exceeded or not
				if thresholdExceeded(metadata[i].Offsets, metadata[currentIndex].Offsets, r.lagThreshold) {
					r.logger.Error("unhealthy node; threshold exceeded", "broker", cfgs[currentIndex].BootstrapBrokers, "threshold", r.lagThreshold)

					// exit the consumer poll loop and wait till consumer poll gets a healthy node
					r.pollCancel()
				}
			}
		}
	}
}

// decrementSourceOffset decrements the offset count for the given topic and partition in the source offsets map.
func (r *Relay) decrementSourceOffset(topic string, partition int32) {
	if topicOffsets, ok := r.srcOffsets[topic]; ok {
		if offset, found := topicOffsets[partition]; found && offset > 0 {
			topicOffsets[partition]--
			r.srcOffsets[topic] = topicOffsets
		}
	}
}

// setSourceOffsets sets the end offsets of the consumer during bootup to exit on consuming everything.
func (r *Relay) setSourceOffsets(of kadm.ListedOffsets) {
	of.Each(func(lo kadm.ListedOffset) {
		ct, ok := r.srcOffsets[lo.Topic]
		if !ok {
			ct = make(map[int32]int64)
		}
		ct[lo.Partition] = lo.Offset
		r.srcOffsets[lo.Topic] = ct
	})

	// read till destination offset; reduce that
	for t, po := range r.destOffsets {
		for p, o := range po {
			if ct, ok := r.srcOffsets[t]; ok {
				if _, found := ct[p]; found {
					ct[p] -= o.EpochOffset().Offset
				}
			}
		}
	}
}
