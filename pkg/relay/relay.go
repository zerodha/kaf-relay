package relay

import (
	"context"
	"log/slog"
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/filter"
)

type RelayCfg struct {
	StopAtEnd bool
}

// relayMetrics holds the per-relay counters that are relay-level concerns
// (as opposed to target-level concerns like flush errors).
type relayMetrics struct {
	candidateSwitches *metrics.Counter

	// Lazy-cached per topic-partition relay counters.
	relayed   map[string]*metrics.Counter
	relayedMu sync.RWMutex
	set       *metrics.Set
}

func newRelayMetrics(set *metrics.Set) relayMetrics {
	return relayMetrics{
		candidateSwitches: set.GetOrCreateCounter(MetricName(MetricCandidateSwitches)),
		relayed:           make(map[string]*metrics.Counter),
		set:               set,
	}
}

// incRelayed increments the relay counter for the given source→target topic-partition.
// Uses double-checked RWMutex locking to lazily cache counters per unique combination.
func (rm *relayMetrics) incRelayed(srcTopic string, srcPartition int32, tgtTopic string, tgtPartition int32) {
	key := srcTopic + ":" + strconv.Itoa(int(srcPartition)) + ":" + tgtTopic + ":" + strconv.Itoa(int(tgtPartition))

	rm.relayedMu.RLock()
	c, ok := rm.relayed[key]
	rm.relayedMu.RUnlock()
	if ok {
		c.Inc()
		return
	}

	rm.relayedMu.Lock()
	c, ok = rm.relayed[key]
	if !ok {
		c = rm.set.GetOrCreateCounter(MetricName(MetricMsgsRelayed,
			Label{"source_topic", srcTopic},
			Label{"source_partition", strconv.Itoa(int(srcPartition))},
			Label{"target_topic", tgtTopic},
			Label{"target_partition", strconv.Itoa(int(tgtPartition))},
		))
		rm.relayed[key] = c
	}
	rm.relayedMu.Unlock()
	c.Inc()
}

// Relay orchestrates consumption from a SourcePool and writing to a Target.
type Relay struct {
	cfg     RelayCfg
	source  *SourcePool
	target  Target
	metrics *metrics.Set
	metr    relayMetrics
	log     *slog.Logger

	topic Topic

	// signalCh is used to signal when the relay poll loop should look for a new healthy server.
	signalCh chan struct{}

	// If stop-at-end is enabled, the "end" offsets of the source
	// read at the time of boot are cached here to compare against
	// live offsets and stop consumption.
	targetOffsets TopicOffsets

	// Live topic offsets from source.
	srcOffsets map[int32]int64

	// list of filter implementations for skipping messages
	filters map[string]filter.Provider
}

func NewRelay(cfg RelayCfg, src *SourcePool, target Target, topic Topic, filters map[string]filter.Provider, m *metrics.Set, log *slog.Logger) (*Relay, error) {
	// If stop-at-end is set, fetch and cache the offsets to determine
	// when end is reached.
	var offsets TopicOffsets
	if cfg.StopAtEnd {
		hwm, err := target.GetHighWatermark(context.Background())
		if err != nil {
			return nil, err
		}
		// Convert relay.Offsets for the target topic to TopicOffsets (map[int32]int64).
		if topicOffsets, ok := hwm[topic.TargetTopic]; ok {
			offsets = topicOffsets
		}
	}

	r := &Relay{
		cfg:     cfg,
		source:  src,
		target:  target,
		metrics: m,
		metr:    newRelayMetrics(m),
		log:     log,

		topic:    topic,
		signalCh: make(chan struct{}, 1),

		srcOffsets:    make(map[int32]int64),
		targetOffsets: offsets,
		filters:       filters,
	}

	return r, nil
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async
func (re *Relay) Start(globalCtx context.Context) error {
	wg := &sync.WaitGroup{}

	// Derive a cancellable context from the global context (which captures kill signals) to use
	// for subsequent connections/health tracking/retries etc.
	ctx, cancel := context.WithCancel(globalCtx)
	defer cancel()

	// Start the indefinite polling health tracker that monitors the current active, topic offset lags etc.
	// and maintains the inventory of servers based on their status. It does not signal or interfere
	// with the relay's consumption. When the relay itself detects an error and asks the pool for a new
	// healthy source server, this inventory is used to pick a healthy source.
	re.log.Info("starting health tracker")
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := re.source.healthcheck(ctx, re.signalCh); err != nil {
			re.log.Error("error tracking healthy nodes", "err", err)
		}
	}()

	// Start the target's background loop (e.g. batching/flushing for Kafka).
	wg.Add(1)
	re.log.Info("starting producer worker")
	go func() {
		defer wg.Done()
		if err := re.target.Start(); err != nil {
			re.log.Error("error starting producer worker", "err", err)
		}
	}()

	// Start the consumer group worker by triggering a signal to the relay loop to fetch
	// a consumer worker to fetch initial healthy node.
	re.log.Info("starting consumer worker")
	// Non-blocking push to avoid getting stuck if the threshold checker goroutine
	// has already sent on the channel concurrently.
	select {
	case re.signalCh <- struct{}{}:
	default:
	}

	wg.Add(1)
	// Relay teardown.
	go func() {
		defer wg.Done()
		// Wait till main ctx is cancelled.
		<-ctx.Done()
	}()

	// Start the indefinite poll that asks for new connections
	// and then consumes messages from them.
	if err := re.startPoll(ctx); err != nil {
		re.log.Error("error starting consumer worker", "err", err)
	}

	// Signal the target to drain and shut down.
	re.target.Close()

	cancel()
	wg.Wait()

	return nil
}

// startPoll starts the consumer worker which polls the kafka cluster for messages.
func (re *Relay) startPoll(ctx context.Context) error {
	var (
		firstPoll = true
		server    *Server
	)

loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-re.signalCh:
			re.metr.candidateSwitches.Inc()
			re.log.Info("poll loop received unhealthy signal. requesting new node")

			for {
				select {
				case <-ctx.Done():
					re.log.Info("poll loop context cancelled before before get node")
					return ctx.Err()
				default:
				}

				// Get returns an availably healthy node with a Kafka connection.
				// This blocks, waits, and retries based on the retry config.
				s, err := re.source.Get(ctx)
				if err != nil {
					re.log.Debug("poll loop could not get healthy node", "error", err)
					continue
				}

				re.log.Info("poll loop got new healthy node", "id", s.ID, "server", s.Config.BootstrapBrokers)
				server = s
				break
			}

		default:
			if re.cfg.StopAtEnd {
				// If relay is configured to sync-until-end (highest offset watermark at the time of relay boot),
				// on the first ever connection (not reconnections) of relay after boot, fetch the highest offsets
				// of the topic to compare.
				if firstPoll {
					of, err := re.source.GetHighWatermark(ctx, server.Client)
					if err != nil {
						re.log.Error("could not get end offsets (first poll); sending unhealthy signal", "id", server.ID, "server", server.Config.BootstrapBrokers, "error", err)
						// Non-blocking push to avoid getting stuck.
						select {
						case re.signalCh <- struct{}{}:
						default:
						}

						continue loop
					}

					srcOffsets := make(map[int32]int64)
					of.Each(func(lo kadm.ListedOffset) {
						srcOffsets[lo.Partition] = lo.Offset
					})

					re.cacheSrcOffsets(srcOffsets)
					firstPoll = false
				}

				if re.hasReachedEnd() {
					re.log.Info("reached end of offsets; stopping relay", "id", server.ID, "server", server.Config.BootstrapBrokers, "offsets", re.srcOffsets)
					return nil
				}
			}

			fetches, err := re.source.GetFetches(server)
			if err != nil {
				re.log.Error("marking server as unhealthy", "server", server.ID)
				// Non-blocking push to avoid getting stuck.
				select {
				case re.signalCh <- struct{}{}:
				default:
				}

				continue loop
			}

			re.log.Debug("received fetches", "len", fetches.NumRecords())
			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				// Always record the latest offsets before the messages are processed for new connections and
				// retries to consume from where it was left off.
				// TODO: What if the next step fails? The messages won't be read again?
				if err := re.source.RecordOffsets(rec); err != nil {
					re.log.Error("error recording offset", "err", err)
					return err
				}

				if err := re.processMessage(ctx, rec); err != nil {
					re.log.Error("error processing message", "err", err)
					return err
				}
			}

			re.log.Debug("processed fetches")
		}
	}
}

// processMessage processes the given message and forwards it to the target.
func (re *Relay) processMessage(ctx context.Context, rec *kgo.Record) error {
	// Decrement the end offsets for the given topic and partition till we reach 0
	if re.cfg.StopAtEnd {
		re.decrementSourceOffset(rec.Partition)
	}

	// If there are filters, run the message through them to decide whether
	// the message has to be skipped or written to the target.
	if re.filters != nil {
		ok := true
		for n, f := range re.filters {
			if !f.IsAllowed(rec.Value) {
				re.log.Debug("filtering message", "message", string(rec.Value), "filter", n)
				re.metrics.GetOrCreateCounter(MetricName(MetricFilteredMsgs, Label{"filter", n})).Inc()
				ok = false
				break
			}
		}

		if !ok {
			return nil
		}
	}

	// Build the relay.Message from the source kgo.Record.
	// Add the source message timestamp as a meta header (_t) so that downstream
	// consumers can compute source→target lag if needed.
	partition := int32(-1) // auto-partition
	if !re.topic.AutoTargetPartition {
		partition = int32(re.topic.TargetPartition)
	}

	msg := Message{
		Key:             rec.Key,
		Value:           rec.Value,
		Topic:           re.topic.TargetTopic,
		Partition:       partition,
		Offset:          rec.Offset,
		SourcePartition: rec.Partition,
		Headers: []Header{
			{Key: "_t", Value: nsToBytes(rec.Timestamp.UnixNano())},
		},
	}

	// Copy any existing headers from the source record.
	for _, h := range rec.Headers {
		msg.Headers = append(msg.Headers, Header{Key: h.Key, Value: h.Value})
	}

	if err := re.target.Write(ctx, msg); err != nil {
		return err
	}

	// Track the relay count per source→target topic-partition pair.
	re.metr.incRelayed(rec.Topic, rec.Partition, re.topic.TargetTopic, partition)

	return nil
}

// decrementSourceOffset decrements the offset count for the given topic and partition in the source offsets map.
func (re *Relay) decrementSourceOffset(partition int32) {
	if offset, found := re.srcOffsets[partition]; found && offset > 0 {
		re.srcOffsets[partition] -= 1
	}
}

// cacheSrcOffsets sets the end offsets of the consumer during bootup to exit on consuming everything.
func (re *Relay) cacheSrcOffsets(of map[int32]int64) {
	re.srcOffsets = of
	// Read till the destination offsets and reduce it from the target weight.
	for p, o := range re.targetOffsets {
		if _, ok := re.srcOffsets[p]; ok {
			re.srcOffsets[p] -= o
		}
	}
}

// hasReachedEnd reports if there is any pending messages in given topic-partition.
func (re *Relay) hasReachedEnd() bool {
	for _, o := range re.srcOffsets {
		if o > 0 {
			return false
		}
	}
	return true
}

func nsToBytes(ns int64) []byte {
	// Preallocate a buffer for the byte slice
	buf := make([]byte, 0, 10) // 10 is enough for most integers
	return strconv.AppendInt(buf, ns, 10)
}
