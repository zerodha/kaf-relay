package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrBrokerUnavailable = errors.New("broker is not available")

	errChosenBrokerDead = "the internal broker struct chosen to issue this request has died--either the broker id is migrating or no longer exists"
)

// consumer is a structure that holds the state and configuration of a Kafka consumer group.
// It manages multiple Kafka clients, handles context cancellation, and tracks offsets for topic partitions.
type consumer struct {
	clients []*kgo.Client
	cfgs    []ConsumerGroupCfg

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

// getCancelFn returns the current context cancel fn for given client index.
func (m *consumerManager) getCancelFn(idx int) context.CancelFunc {
	return m.c.cancelFn[idx]
}

// getCurrentClient returns current client.
func (m *consumerManager) getCurrentClient() *kgo.Client {
	return m.c.clients[m.c.idx]
}

// setCurrentClient sets client into current index.
func (m *consumerManager) setCurrentClient(cl *kgo.Client) {
	m.c.clients[m.c.idx] = cl
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
	if !up {
		return fmt.Errorf("%w: %w", ErrBrokerUnavailable, err)
	}

	// Create a new context for this consumer group poll fetch loop
	ctx, cancel := context.WithCancel(m.c.parentCtx)
	m.setCurrentContext(ctx, cancel)

	// XXX: Offsets can only be reset for `Empty` consumer groups
	// CASE1: client connection exist
	//		- leave the group and reset the offsets (if there is any)
	//		- reinitialize the consumer group; ready!
	// CASE2: client does not exist
	//		- create a new consumer group
	//		- leave the group and reset the offsets (if there is any)
	//		- reinitialize the consumer group if we had left the group before; ready!
	var (
		reinit     bool
		retries    = 0
		maxRetries = 3
	)
initConsumer:
	for retries < maxRetries {
		if cl != nil {
			cl.ForceMetadataRefresh()

			reinit = true
			l.Info("reusing consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)

			err := leaveAndResetOffsets(ctx, cl, cfg, m.c.offsets, l)
			if err != nil {
				if err.Error() == errChosenBrokerDead {
					l.Info("faulty existing client conn; reiniting consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID, "retries", retries)
					cl, err = m.initKafkaConsumerGroup()
					if err != nil {
						return err
					}

					retries++
					continue initConsumer
				}

				l.Error("error leave and reset offsets", "err", err)
				return err
			}

			// reset offsets went through; break the loop and we can reinitialize the consumer group
			break initConsumer
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

				err := leaveAndResetOffsets(ctx, cl, cfg, m.c.offsets, l)
				if err != nil {
					if err.Error() == errChosenBrokerDead {
						l.Info("faulty existing client conn; reiniting consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID, "retries", retries)
						cl, err = m.initKafkaConsumerGroup()
						if err != nil {
							return err
						}

						retries++
						continue initConsumer
					}

					l.Error("error leave and reset offsets", "err", err)
					return err
				}
			}

			// reset offsets went through; break the loop and we can reinitialize the consumer group
			break initConsumer
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

	// test connectivity and ensures the source topics exists
	if err := testConnection(cl, cfg.SessionTimeout, cfg.Topics); err != nil {
		return err
	}

	return nil
}

// initConsumer initalizes the consumer when the programs boots up.
func initConsumer(ctx context.Context, m *consumerManager, cfg Config, o kadm.ListedOffsets, l *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	c := consumer{
		cfgs:      cfg.Consumers,
		logger:    l,
		parentCtx: ctx,
		ctx:       make([]context.Context, len(cfg.Consumers)),
		cancelFn:  make([]context.CancelFunc, len(cfg.Consumers)),
		clients:   make([]*kgo.Client, len(cfg.Consumers)),
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

	// setup destination offsets before connecting to consumer
	m.setOffsets(o.KOffsets())

	for retries < cfg.App.MaxFailovers || cfg.App.MaxFailovers == IndefiniteRetry {
		for i := 0; i < len(cfg.Consumers); i++ {
			// check for context cancellations
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			l.Info("creating consumer group", "broker", cfg.Consumers[idx].BootstrapBrokers, "group_id", cfg.Consumers[idx].GroupID)
			if err = m.connectToNextNode(); err != nil {
				l.Error("error creating consumer", "err", err)
				retries++
				waitTries(ctx, backoff(retries))

				// Round robin select consumer config id
				idx = (idx + 1) % len(cfg.Consumers)
			} else {
				break
			}
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
