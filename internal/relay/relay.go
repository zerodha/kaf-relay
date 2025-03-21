package relay

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/filter"
)

type RelayCfg struct {
	StopAtEnd bool
}

// Relay represents the input, output kafka and the remapping necessary to forward messages from one topic to another.
type Relay struct {
	cfg    RelayCfg
	source *SourcePool
	target *Target
	log    *slog.Logger

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

func NewRelay(cfg RelayCfg, src *SourcePool, target *Target, topic Topic, filters map[string]filter.Provider, log *slog.Logger) (*Relay, error) {
	// If stop-at-end is set, fetch and cache the offsets to determine
	// when end is reached.
	var offsets TopicOffsets
	if cfg.StopAtEnd {
		if o, err := target.GetHighWatermark(); err != nil {
			return nil, err
		} else {
			offsets = o.KOffsets()[topic.TargetTopic]
		}
	}

	r := &Relay{
		cfg:    cfg,
		source: src,
		target: target,
		log:    log,

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

	// Start the target / producer controller.
	wg.Add(1)
	re.log.Info("starting producer worker")
	go func() {
		defer wg.Done()
		if err := re.target.Start(); err != nil {
			re.log.Error("error starting producer worker", "err", err)
		}
	}()

	// Start the consumer group worker by trigger a signal to the relay loop to fetch
	// a consumer worker to fetch initial healthy node.
	re.log.Info("starting consumer worker")
	// The push is non-blocking to avoid getting stuck trying to send on the poll loop
	// if the threshold checker go-routine might have already sent on the channel concurrently.
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

	// Close the producer inlet channel.
	close(re.target.inletCh)

	// Close producer.
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
				// pn the first ever connection (not reconnections) of relay after boot, fetch the highest offsets
				// of the topic to compare.
				if firstPoll {
					of, err := re.source.GetHighWatermark(ctx, server.Client)
					if err != nil {
						re.log.Error("could not get end offsets (first poll); sending unhealthy signal", "id", server.ID, "server", server.Config.BootstrapBrokers, "error", err)
						// The push is non-blocking to avoid getting stuck trying to send on the poll loop
						// if the threshold checker go-routine might have already sent on the channel concurrently.
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
				// The push is non-blocking to avoid getting stuck trying to send on the poll loop
				// if the threshold checker go-routine might have already sent on the channel concurrently.
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

// processMessage processes the given message and forwards it to the producer batch channel.
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
				ok = false
				break
			}
		}

		if !ok {
			return nil
		}
	}

	// Add the src message time as a meta header to the target.
	// The target consumer can check the lag between the src and target message time if required.
	// Repurpose &kgo.Record and forward it to producer to reduce allocs.
	rec.Headers = append(rec.Headers, kgo.RecordHeader{
		Key:   "_t",
		Value: nsToBytes(rec.Timestamp.UnixNano()),
	})
	rec.Timestamp = time.Time{}
	rec.Topic = re.topic.TargetTopic
	if !re.topic.AutoTargetPartition {
		rec.Partition = int32(re.topic.TargetPartition)
	}
	rec.Attrs = kgo.RecordAttrs{}
	rec.ProducerEpoch = 0
	rec.ProducerID = 0
	rec.LeaderEpoch = 0
	rec.Offset = 0
	rec.Context = nil

	// Queue the message for writing to target.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case re.target.GetBatchCh() <- rec:
	default:
		re.log.Error("target inlet channel blocked")
		re.target.GetBatchCh() <- rec
	}

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
			re.srcOffsets[p] -= o.EpochOffset().Offset
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
