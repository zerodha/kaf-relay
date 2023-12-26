package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrBrokerUnavailable = errors.New("broker is not available")
)

// consumer is a structure that holds the state and configuration of a Kafka consumer group.
// It manages multiple Kafka clients, handles context cancellation, and tracks offsets for topic partitions.
type consumer struct {
	client *kgo.Client
	cfgs   []ConsumerGroupCfg

	nextIndex int
	idx       int

	logger *slog.Logger

	parentCtx context.Context
	ctx       []context.Context
	cancelFn  []context.CancelFunc

	offsets map[string]map[int32]kgo.Offset
}

// consumerManager is a structure that manages a Kafka consumer instance.
// It handles consumer operations and manages the state of reconnections.
type consumerManager struct {
	// protect consumer from concurrent access
	mu sync.Mutex
	c  *consumer

	reconnectInProgress uint32

	// single/failover
	mode string
}

// Lock locks the consumer within consumer manager
func (m *consumerManager) Lock() {
	m.mu.Lock()
}

// Lock unlocks the consumer within consumer manager
func (m *consumerManager) Unlock() {
	m.mu.Unlock()
}

// index returns the current consumer group index
func (m *consumerManager) Index() int {
	return m.c.idx
}

// incrementNodeIndex picks the next consumer group config
func (m *consumerManager) incrementIndex() int {
	m.c.nextIndex++
	m.c.idx = (m.c.nextIndex - 1) % len(m.c.cfgs)
	return m.c.idx
}

// getOffsets returns the current offsets
func (m *consumerManager) getOffsets() map[string]map[int32]kgo.Offset {
	return m.c.offsets
}

// setTopicOffsets is responsible for maintaining topic wise offsets in memory.
// It updates the topic-offset map for the consumer. This map is the source of truth
// for syncing offsets across consumers (in failover mode). It also mark commits each record.
// In the case of `single` mode there will auto-committing that will routinely commit the marked
// records. For `failover` mode there is no auto-committing and the marked offsets are only committed
// to kafka once the program is shutdown. The marked offsets are committed in the `OnPartitionsRevoked`
// consumer callback.
func (m *consumerManager) setTopicOffsets(rec *kgo.Record) {
	// We only commit records in normal mode.
	oMap := make(map[int32]kgo.Offset)
	oMap[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)

	// keep offsets in memory
	if m.c.offsets != nil {
		if o, ok := m.c.offsets[rec.Topic]; ok {
			o[rec.Partition] = oMap[rec.Partition]
			m.c.offsets[rec.Topic] = o
		} else {
			m.c.offsets[rec.Topic] = oMap
		}
	} else {
		m.c.offsets = make(map[string]map[int32]kgo.Offset)
		m.c.offsets[rec.Topic] = oMap
	}

	cl := m.getCurrentClient()
	if cl == nil {
		m.c.logger.Error("consumer not initialized", "index", m.c.idx, "brokers", m.c.cfgs[m.c.idx].BootstrapBrokers)
		return
	}

	cl.MarkCommitRecords(rec)
}

// setOffsets set the current offsets into relay struct
func (m *consumerManager) setOffsets(o map[string]map[int32]kgo.Offset) {
	m.c.offsets = o
}

// getCurrentConfig returns the current consumer group config.
func (m *consumerManager) getCurrentConfig() ConsumerGroupCfg {
	return m.c.cfgs[m.c.idx]
}

// getClient returns the client for given index.
func (m *consumerManager) getClient(idx int) *kgo.Client {
	return m.c.client
}

// getCancelFn returns the current context cancel fn for given client index.
func (m *consumerManager) getCancelFn(idx int) context.CancelFunc {
	return m.c.cancelFn[idx]
}

// getCurrentClient returns current client.
func (m *consumerManager) getCurrentClient() *kgo.Client {
	return m.c.client
}

// setCurrentClient sets client into current index.
func (m *consumerManager) setCurrentClient(cl *kgo.Client) {
	m.c.client = cl
}

// getCurrentContext returns the current context, cancellation fns.
func (m *consumerManager) getCurrentContext() (context.Context, context.CancelFunc) {
	return m.c.ctx[m.c.idx], m.c.cancelFn[m.c.idx]
}

// setCurrentContext sets the current context, cancellation fns.
func (m *consumerManager) setCurrentContext(ctx context.Context, cancel context.CancelFunc) {
	m.c.ctx[m.c.idx] = ctx
	m.c.cancelFn[m.c.idx] = cancel
}

// setActive assigns current active index inside consumer manager
func (m *consumerManager) setActive(idx int) {
	m.c.idx = idx
	m.c.nextIndex = idx + 1
}

// func (m *consumerManager) validateOffsets(ctx context.Context) error {
// 	var (
// 		consTopics []string
// 		prodTopics []string
// 	)
// 	for c, p := range r.topics {
// 		consTopics = append(consTopics, c)
// 		prodTopics = append(prodTopics, p)
// 	}

// 	c := m.getCurrentClient()
// 	consOffsets, err := getEndOffsets(ctx, c, consTopics)
// 	if err != nil {
// 		return err
// 	}

// 	prodOffsets, err := getEndOffsets(ctx, r.producer.client, prodTopics)
// 	if err != nil {
// 		return err
// 	}

// 	for _, ps := range consOffsets {
// 		for _, o := range ps {
// 			// store the end offsets
// 			if r.stopAtEnd {
// 				ct, ok := r.endOffsets[o.Topic]
// 				if !ok {
// 					ct = make(map[int32]int64)
// 				}
// 				ct[o.Partition] = o.Offset
// 				r.endOffsets[o.Topic] = ct
// 			}

// 			// Check if mapping exists
// 			t, ok := r.topics[o.Topic]
// 			if !ok {
// 				return fmt.Errorf("error finding destination topic for %v in given mapping", o.Topic)
// 			}

// 			// Check if topic, partition exists in destination
// 			destOffset, ok := prodOffsets.Lookup(t, o.Partition)
// 			if !ok {
// 				return fmt.Errorf("error finding destination topic, partition for %v in destination kafka", o.Topic)
// 			}

// 			// Confirm that committed offsets of consumer group matches the offsets of destination kafka topic partition
// 			if destOffset.Offset > o.Offset {
// 				return fmt.Errorf("destination topic(%v), partition(%v) offsets(%v) is higher than consumer group committed offsets(%v)",
// 					destOffset.Topic, destOffset.Partition, destOffset.Offset, o.Offset)
// 			}
// 		}
// 	}

// 	return nil
// }

// connect selects the the configuration (round-robin fashion) and proceeds to create
func (m *consumerManager) connectToNextNode() error {
	// select consumer config in round-robin fashion
	m.incrementIndex()

	var (
		cfg  = m.getCurrentConfig()
		cl   = m.getCurrentClient()
		l    = m.c.logger
		err  error
		conn net.Conn
	)

	l.Info("attempting to connect to broker", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
	up := false
	for _, addr := range cfg.BootstrapBrokers {
		if conn, err = net.DialTimeout("tcp", addr, time.Second); err != nil && checkErr(err) {
			up = false
		} else {
			up = true
			conn.Close()
			break
		}
	}

	// Return err if we dont find atleast 1 broker available in the bootstrap broker list
	if !up && err != nil {
		return fmt.Errorf("%w: %w", ErrBrokerUnavailable, err)
	}

	// Create a new context for this consumer group poll fetch loop
	ctx, cancel := context.WithCancel(m.c.parentCtx)
	m.setCurrentContext(ctx, cancel)

	var reinit bool
	if cl != nil {
		reinit = true
		l.Info("reusing consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)

		if err := leaveAndResetOffsets(ctx, cl, cfg, m.c.offsets, l); err != nil {
			l.Error("error leave and reset offsets", "err", err)
			return err
		}
	} else {
		l.Info("creating consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
		cl, err = m.initKafkaConsumerGroup()
		if err != nil {
			return err
		}

		// Reset consumer group offsets using the existing offsets
		if m.c.offsets != nil {
			reinit = true
			// pause and close the group to mark the group as `Empty` (non-active) as resets are not allowed for `Stable` (active) consumer groups.
			cl.PauseFetchTopics(cfg.Topics...)

			if err := leaveAndResetOffsets(ctx, cl, cfg, m.c.offsets, l); err != nil {
				l.Error("error leave and reset offsets", "err", err)
				return err
			}
		}
	}

	// No offsets in memory; we aren't required to reinit as a clean client is already there.
	if reinit {
		l.Info("reinitializing consumer group", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
		cl, err = m.initKafkaConsumerGroup()
		if err != nil {
			return err
		}

		l.Info("resuming consumer fetch topics", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID, "topics", cfg.Topics)
		cl.ResumeFetchTopics(cfg.Topics...)
	}

	// Replace the current client index with new client
	m.setCurrentClient(cl)

	// reset offsets
	m.c.offsets = nil

	// test connectivity and ensures the source topics exists
	if err := testConnection(cl, cfg.SessionTimeout, cfg.Topics); err != nil {
		return err
	}

	return nil
}

// initConsumer initalizes the consumer when the programs boots up.
func initConsumer(ctx context.Context, m *consumerManager, cfgs []ConsumerGroupCfg, maxRetries int, l *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	c := consumer{
		cfgs:      cfgs,
		logger:    l,
		parentCtx: ctx,
		ctx:       make([]context.Context, len(cfgs)),
		cancelFn:  make([]context.CancelFunc, len(cfgs)),
	}

	c.ctx = append(c.ctx, ctx)
	c.cancelFn = append(c.cancelFn, cancel)

	// set consumer under manager
	m.c = &c

	// try connecting to the consumers one by one
	// exit if none of the given nodes are available.
	var (
		err     error
		idx     = 0
		retries = 0
		backoff = retryBackoff()
	)

	for retries < maxRetries || maxRetries == IndefiniteRetry {
		for i := 0; i < len(cfgs); i++ {
			l.Info("creating consumer group", "broker", cfgs[idx].BootstrapBrokers, "group_id", cfgs[idx].GroupID)
			if err = m.connectToNextNode(); err != nil {
				l.Error("error creating consumer", "err", err)
				if errors.Is(err, ErrBrokerUnavailable) {
					retries++
					waitTries(ctx, backoff(retries))
				}

				// Round robin select consumer config id
				idx = (idx + 1) % len(cfgs)
				continue
			}

			//break
		}

		// During this ith attempt we were able to connect to
		// at least 1 consumer part of the list
		if err == nil {
			break
		}
	}

	// return error if none of the brokers are available
	if err != nil {
		return err
	}

	// set the default active consumer group
	// TODO: Close other consumer groups?
	m.setActive(idx)

	return nil
}

// func checkHealthy(ctx context.Context, m *consumerManager, cfgs []ConsumerGroupCfg, maxRetries int, l *slog.Logger) error {
// 	var (
// 		err error
// 		//brokersUp = make(map[string]struct{})
// 		idx     = 0
// 		retries = 0
// 		backoff = retryBackoff()
// 	)

// 	for retries < maxRetries || maxRetries == IndefiniteRetry {
// 		l.Info("creating consumer group", "broker", cfgs[idx].BootstrapBrokers, "group_id", cfgs[idx].GroupID)
// 		if err = m.connectToNextNode(); err != nil {
// 			l.Error("error creating consumer", "err", err)
// 			if errors.Is(err, ErrBrokerUnavailable) {
// 				retries++
// 				waitTries(ctx, backoff(retries))
// 			}

// 			// Round robin select consumer config id
// 			idx = (idx + 1) % len(cfgs)
// 			continue
// 		}

// 		// mark the consumer group that is up
// 		//brokersUp[cfgs[idx].GroupID] = struct{}{}
// 		break
// 	}

// 	// return error if none of the brokers are available
// 	if err != nil {
// 		return err
// 	}

// 	// set the default active consumer group
// 	// TODO: Close other consumer groups?
// 	m.setActive(idx)

// 	return nil
// }
