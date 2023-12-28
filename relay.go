package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/joeirimpan/kaf-relay/filter"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// relay represents the input, output kafka and the remapping necessary to forward messages from one topic to another.
type relay struct {
	consumer *consumer
	producer *producer

	unhealthyCh chan struct{}

	topics  map[string]string
	metrics *metrics.Set
	logger  *slog.Logger

	// retry
	retryBackoffFn func(int) time.Duration
	maxRetries     int
	retries        int

	// monitor lags
	lagThreshold   int64
	lagMonitorFreq time.Duration

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
	pollCancel func()
}

// getMetrics writes the internal prom metrics to the given io.Writer
func (r *relay) getMetrics(buf io.Writer) {
	r.metrics.WritePrometheus(buf)
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async
func (r *relay) Start(ctx context.Context) error {
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
			r.logger.Error("error managing offsets")
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

	return nil
}

// Close close the underlying kgo.Client(s)
func (r *relay) Close() {
	r.consumer.Close()
	r.producer.Close()
}

// startConsumerWorker starts the consumer worker which polls the kafka cluster for messages.
func (r *relay) startConsumerWorker(ctx context.Context) error {
	pollCtx, pollCancelFn := context.WithCancel(context.Background())
	r.pollCancel = pollCancelFn

	var nodeID = -1
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
			r.logger.Debug("poll loop received unhealthy signal")
			pollCancelFn()

			var (
				succ = false
				err  error
			)
			for !succ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				r.logger.Debug("poll loop requesting healthy node")
				nodeID, err = r.consumer.GetHealthy(ctx)
				if err != nil {
					r.logger.Error("poll loop could not get healthy node", "err", err)
					r.retries++
					waitTries(ctx, r.retryBackoffFn(r.retries))
					continue
				}

				r.logger.Debug("poll loop got healthy node", "id", nodeID)
				err = r.consumer.Connect(ctx, r.consumer.cfgs[nodeID])
				if err != nil {
					r.logger.Error("poll loop could not re-init consumer", "err", err)
					r.retries++
					waitTries(ctx, r.retryBackoffFn(r.retries))
					continue
				}
				succ = true
			}

			pollCtx, pollCancelFn = context.WithCancel(ctx)
			r.pollCancel = pollCancelFn
			defer pollCancelFn()

			r.logger.Debug("poll loop sending current node", "id", nodeID)
			r.nodeCh <- nodeID

		default:
			cl := r.consumer.client

			// Stop the poll loop if we reached the end of offsets fetched on boot.
			if r.stopAtEnd {
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
				r.unhealthyCh <- struct{}{}

				r.logger.Debug("updating weight", "node", nodeID, "weight", -1)
				r.consumer.nodeTracker.PushDown(nodeID)

				continue pollLoop
			}

			// Proxy fetch errors into error callback function
			errs := fetches.Errors()
			for _, err := range errs {
				r.retries++
				waitTries(ctx, r.retryBackoffFn(r.retries))
				r.logger.Error("fetch errors; sending unhealthy signal", "err", err.Err)

				r.unhealthyCh <- struct{}{}

				r.logger.Debug("updating weight", "node", nodeID, "weight", -1)
				r.consumer.nodeTracker.PushDown(nodeID)
				continue pollLoop
			}

			// reset max retries if successfull
			r.retries = 0

			// iter through messages
			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				oMap := make(map[int32]kgo.Offset)
				oMap[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
				if r.consumer.offsetMgr.Offsets != nil {
					if o, ok := r.consumer.offsetMgr.Offsets[rec.Topic]; ok {
						o[rec.Partition] = oMap[rec.Partition]
						r.consumer.offsetMgr.Offsets[rec.Topic] = o
					} else {
						r.consumer.offsetMgr.Offsets[rec.Topic] = oMap
					}
				} else {
					r.consumer.offsetMgr.Offsets = make(map[string]map[int32]kgo.Offset)
					r.consumer.offsetMgr.Offsets[rec.Topic] = oMap
				}

				if err := r.processMessage(ctx, rec); err != nil {
					r.logger.Error("error processing message", "err", err)
					return err
				}
			}
		}
	}
}

// processMessage processes the given message and forwards it to the producer batch channel.
func (r *relay) processMessage(ctx context.Context, rec *kgo.Record) error {
	// Decrement the end offsets for the given topic, partition till we reach 0
	if r.stopAtEnd {
		if ct, ok := r.srcOffsets[rec.Topic]; ok {
			o := ct[rec.Partition]
			if o > 0 {
				o -= 1
				ct[rec.Partition] = o
				r.srcOffsets[rec.Topic] = ct
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
// * Push a healthy node to the channel if threshold is not exceeded; always drain the existing and repush if full
// * Push a signal to exit the poll loop if threshold exceeded
func (r *relay) trackHealthy(ctx context.Context) error {
	tick := time.NewTicker(r.lagMonitorFreq)
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

	var (
		curr        = 0
		isFirstNode = true
	)
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

		case nodeID := <-r.nodeCh:
			r.logger.Info("tracker received current node from poll loop", "id", nodeID)
			curr = nodeID

		case <-tick.C:
			r.logger.Debug("checking topic lag", "freq", r.lagMonitorFreq.Seconds())
			var (
				n           = len(cfgs)
				cl          *kgo.Client
				currOffsets kadm.ListedOffsets
				err         error
			)

			// check in the next iteration
			currOffsets, err = getEndOffsets(ctx, clients[curr], cfgs[curr].Topics, r.maxReqTime)
			if err != nil && currOffsets == nil {
				r.logger.Debug("error fetching end offset for current client", "err", err, "node", curr)
				r.consumer.nodeTracker.PushDown(curr)
			}

			for i := 0; i < n; i++ {
				r.logger.Debug("tracker checking node", "id", i, "curr", curr)
				cfg := cfgs[i]
				cl = clients[i]

				// create a new client if it doesn't exist
				if cl == nil {
					var err error
					cl, err = getClient(cfg)
					if err != nil {
						r.logger.Error("error creating admin client for tracking lag", "err", err)
						r.consumer.nodeTracker.PushDown(i)
						continue
					}

					clients[i] = cl
				}

				// get end offsets using admin api
				of, err := getEndOffsets(ctx, cl, cfg.Topics, r.maxReqTime)
				if err != nil {
					r.logger.Debug("error fetching end offsets; tracking topic lag", "err", err)
					r.consumer.nodeTracker.PushDown(i)
					continue
				}

				var weight int
				for _, v := range of {
					for _, o := range v {
						weight += int(o.Offset)
					}
				}

				r.logger.Debug("updating weight", "node", i, "weight", weight)
				r.consumer.nodeTracker.PushUp(i, weight)

				// set end offsets
				if isFirstNode && r.stopAtEnd {
					r.logger.Debug("setting up consumer offsets to exit automatically on consuming everything")
					// set end offsets of consumer during bootup to exit on consuming everything.
					for _, ps := range of {
						for _, o := range ps {
							ct, ok := r.srcOffsets[o.Topic]
							if !ok {
								ct = make(map[int32]int64)
							}
							ct[o.Partition] = o.Offset
							r.srcOffsets[o.Topic] = ct
						}
					}

					isFirstNode = false
				}

				// skip lag check for current node
				if r.lagThreshold == 0 || currOffsets == nil || i == curr {
					continue
				}

				// check if threshold is exceeded or not
				if thresholdExceeded(of, currOffsets, r.lagThreshold) {
					//select {
					// send the healthy node to the channel;
					//case r.unhealthyCh <- struct{}{}:
					r.logger.Error("sending unhealthy: threshold exceeded")
					r.pollCancel()
					curr = <-r.nodeCh

					continue
				}
			}
		}
	}
}
