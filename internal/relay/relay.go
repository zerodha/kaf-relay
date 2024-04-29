package relay

import (
	"context"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/filter"
)

type RelayCfg struct {
	StopAtEnd bool `koanf:"stop_at_end"`
}

// Relay represents the input, output kafka and the remapping necessary to forward messages from one topic to another.
type Relay struct {
	cfg    RelayCfg
	source *SourcePool
	target *Target
	log    *slog.Logger

	topics map[string]Topic

	// signalCh is used to signal when the relay poll loop should look for a new healthy server.
	signalCh chan struct{}

	// If stop-at-end is enabled, the "end" offsets of the source
	// read at the time of boot are cached here to compare against
	// live offsets and stop consumption.
	targetOffsets map[string]map[int32]kgo.Offset

	// Live topic offsets from source.
	srcOffsets map[string]map[int32]int64

	// list of filter implementations for skipping messages
	filters map[string]filter.Provider
}

func NewRelay(cfg RelayCfg, src *SourcePool, target *Target, topics map[string]Topic, filters map[string]filter.Provider, log *slog.Logger) (*Relay, error) {
	// If stop-at-end is set, fetch and cache the offsets to determine
	// when end is reached.
	var offsets kadm.ListedOffsets
	if cfg.StopAtEnd {
		if o, err := target.GetHighWatermark(); err != nil {
			return nil, err
		} else {
			offsets = o
		}
	}

	r := &Relay{
		cfg:    cfg,
		source: src,
		target: target,
		log:    log,

		topics:   topics,
		signalCh: make(chan struct{}, 1),

		srcOffsets:    make(map[string]map[int32]int64),
		targetOffsets: offsets.KOffsets(),
		filters:       filters,
	}

	return r, nil
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async
func (re *Relay) Start(globalCtx context.Context) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

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

	// start producer worker
	wg.Add(1)
	re.log.Info("starting producer worker")
	go func() {
		defer wg.Done()
		if err := re.target.Start(ctx); err != nil {
			re.log.Error("error starting producer worker", "err", err)
		}

		if ctx.Err() != context.Canceled {
			cancel()
		}
	}()

	// Start consumer group
	re.log.Info("starting consumer worker")

	// Trigger consumer to fetch initial healthy node.
	re.signalCh <- struct{}{}

	// Start the indefinite poll that asks for new connections
	// and then consumes messages from them.
	if err := re.startPoll(ctx); err != nil {
		re.log.Error("error starting consumer worker", "err", err)
	}

	// Close the target/producer on exit.
	re.target.CloseBatchCh()

	wg.Wait()

	return nil
}

// Close close the underlying kgo.Client(s)
func (re *Relay) Close() {
	re.log.Debug("closing relay consumer, producer...")
	re.source.Close()
	re.target.Close()
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

				server = s
				break
			}

		default:
			if re.cfg.StopAtEnd {
				// If relay is configured to sync-until-end (highest offset watermark at the time of relay boot),
				// pn the first ever connection (not reconnections) of relay after boot, fetch the highest offsets
				// of the topic to compare.
				if firstPoll {
					of, err := re.source.GetHighWatermark(ctx, server.Client)
					if err != nil {
						re.log.Error("could not get end offsets (first poll); sending unhealthy signal", "id", server.ID, "server", server.Config.BootstrapBrokers, "error", err)
						re.signalCh <- struct{}{}

						continue loop
					}

					re.cacheSrcOffsets(of)
					firstPoll = false
				}

				if re.hasReachedEnd() {
					re.log.Info("reached end of offsets; stopping relay", "id", server.ID, "server", server.Config.BootstrapBrokers, "offsets", re.srcOffsets)
					return nil
				}
			}

			fetches, err := re.source.GetFetches(server)
			if err != nil {
				re.log.Debug("marking server as unhealthy", "server", server.ID)
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				// Always record the latest offsets before the messages are processed for new connections and
				// retries to consume from where it was left off.
				// NOTE: What if the next step fails? The messages won't be read again?
				re.source.RecordOffsets(rec)

				if err := re.processMessage(ctx, rec); err != nil {
					re.log.Error("error processing message", "err", err)
					return err
				}
			}

			server.Client.AllowRebalance()
		}
	}
}

// processMessage processes the given message and forwards it to the producer batch channel.
func (re *Relay) processMessage(ctx context.Context, rec *kgo.Record) error {
	// Decrement the end offsets for the given topic and partition till we reach 0
	if re.cfg.StopAtEnd {
		re.decrementSourceOffset(rec.Topic, rec.Partition)
	}

	// check if message needs to skipped?
	msgAllowed := true
	for n, f := range re.filters {
		if !f.IsAllowed(rec.Value) {
			re.log.Debug("filtering message", "message", string(rec.Value), "filter", n)
			msgAllowed = false
			break
		}
	}

	// skip message
	if !msgAllowed {
		return nil
	}

	// Fetch destination topic. Ignore if remapping is not defined.
	t, ok := re.topics[rec.Topic]
	if !ok {
		return nil
	}

	// queue the message for producing in batch
	select {
	case <-ctx.Done():
		return ctx.Err()

	case re.target.GetBatchCh() <- &kgo.Record{
		Key:       rec.Key,
		Value:     rec.Value,
		Topic:     t.TargetTopic, // remap destination topic
		Partition: rec.Partition,
	}:

		// default:
		// 	r.logger.Debug("producer batch channel full")
	}

	return nil
}

// decrementSourceOffset decrements the offset count for the given topic and partition in the source offsets map.
func (re *Relay) decrementSourceOffset(topic string, partition int32) {
	if topicOffsets, ok := re.srcOffsets[topic]; ok {
		if offset, found := topicOffsets[partition]; found && offset > 0 {
			topicOffsets[partition]--
			re.srcOffsets[topic] = topicOffsets
		}
	}
}

// cacheSrcOffsets sets the end offsets of the consumer during bootup to exit on consuming everything.
func (re *Relay) cacheSrcOffsets(of kadm.ListedOffsets) {
	of.Each(func(lo kadm.ListedOffset) {
		ct, ok := re.srcOffsets[lo.Topic]
		if !ok {
			ct = make(map[int32]int64)
		}
		ct[lo.Partition] = lo.Offset
		re.srcOffsets[lo.Topic] = ct
	})

	// read till destination offset; reduce that.
	for t, po := range re.targetOffsets {
		for p, o := range po {
			if ct, ok := re.srcOffsets[t]; ok {
				if _, found := ct[p]; found {
					re.srcOffsets[t][p] -= o.EpochOffset().Offset
				}
			}
		}
	}
}

// hasReachedEnd reports if there is any pending messages in given topic-partition
func (re *Relay) hasReachedEnd() bool {
	for _, p := range re.srcOffsets {
		for _, o := range p {
			if o > 0 {
				return false
			}
		}
	}

	return true
}
