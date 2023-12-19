package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrBrokerUnavailable = errors.New("broker is not available")
)

// consumerManager is a structure that manages a Kafka consumer instance.
// It handles consumer operations and manages the state of reconnections.
type consumerManager struct {
	// protect consumer from concurrent access
	mu sync.Mutex
	c  *consumer

	reconnectInProgress uint32

	// single/failover
	mode      string
	brokersUp map[string]struct{}
}

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

	offsets map[string]map[int32]kgo.EpochOffset
}

func (m *consumerManager) Lock() {
	m.mu.Lock()
}

func (m *consumerManager) Unlock() {
	m.mu.Unlock()
}

func (m *consumerManager) Index() int {
	return m.c.idx
}

// getOffsets returns the current offsets
func (m *consumerManager) getOffsets() map[string]map[int32]kgo.EpochOffset {
	return m.c.offsets
}

// SetTopicOffsets is responsible for maintaining topic wise offsets in memory.
// It updates the topic-offset map for the consumer. This map is the source of truth
// for syncing offsets across consumers (in failover mode). It also mark commits each record.
// In the case of `single` mode there will auto-committing that will routinely commit the marked
// records. For `failover` mode there is no auto-committing and the marked offsets are only committed
// to kafka once the program is shutdown. The marked offsets are committed in the `OnPartitionsRevoked`
// consumer callback.
func (m *consumerManager) SetTopicOffsets(rec *kgo.Record) {
	// We only commit records in normal mode.
	oMap := make(map[int32]kgo.EpochOffset)
	oMap[rec.Partition] = kgo.EpochOffset{
		Epoch:  rec.LeaderEpoch,
		Offset: rec.Offset + 1,
	}

	m.Lock()
	defer m.Unlock()
	// keep offsets in memory
	if m.c.offsets != nil {
		m.c.offsets[rec.Topic] = oMap
	} else {
		m.c.offsets = make(map[string]map[int32]kgo.EpochOffset)
		m.c.offsets[rec.Topic] = oMap
	}

	m.getCurrentClient().MarkCommitRecords(rec)
}

// setOffsets set the current offsets into relay struct
func (m *consumerManager) setOffsets(o map[string]map[int32]kgo.EpochOffset) {
	m.c.offsets = o
}

// getCurrentConfig returns the current consumer group config.
func (m *consumerManager) getCurrentConfig() ConsumerGroupCfg {
	return m.c.cfgs[m.c.idx]
}

// getClient returns the client for given index.
func (m *consumerManager) getClient(idx int) *kgo.Client {
	return m.c.clients[idx]
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

// nodeIncr picks the next consumer group config
func (c *consumer) nodeIncr() int {
	c.nextIndex++
	c.idx = (c.nextIndex - 1) % len(c.cfgs)
	return c.idx
}

// consumerHook struct implements the hook interface (now, OnBrokerDisconnect)
type consumerHook struct {
	m *consumerManager

	// retries
	retryBackoffFn func(int) time.Duration
	maxRetries     int
	retries        int
}

// OnBrokerDisconnect is a callback function that handles broker disconnection events in a Kafka consumer group.
// It checks the disconnection status, verifies the broker's state, and initiates reconnection if necessary.
func (h *consumerHook) OnBrokerDisconnect(meta kgo.BrokerMetadata, conn net.Conn) {
	// Prevent concurrent access of consumer manager when a reconnect is in progress
	if !atomic.CompareAndSwapUint32(&h.m.reconnectInProgress, StateDisconnected, StateConnecting) {
		h.m.c.logger.Debug("reconnect in progress; ignore OnBrokerDisconnect callback")
		return
	}

	// flip the bit back to initial state
	defer atomic.CompareAndSwapUint32(&h.m.reconnectInProgress, StateConnecting, StateDisconnected)

	// lock before we attempting to replace the underlying client
	h.m.Lock()
	defer h.m.Unlock()

	var (
		cl          = h.m.getCurrentClient()
		cfg         = h.m.getCurrentConfig()
		ctx, cancel = h.m.getCurrentContext()
		l           = h.m.c.logger
	)

	// failover only allowed for manual commits because autocommit goroutine cannot be stopped otherwise
	if cfg.OffsetCommitInterval != 0 {
		l.Debug("failover not allowed for consumer group autocommits")
		return
	}

	// ignore if master ctx is closed (keyboard interrupt!)
	select {
	case <-h.m.c.parentCtx.Done():
		return
	case <-ctx.Done():
		return
	default:
	}

	addr := net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port)))
	// OnBrokerDisconnect gets triggered 3 times. Ignore the subsequent ones.
	if !inSlice(addr, cfg.BootstrapBrokers) {
		l.Debug(fmt.Sprintf("%s is not current active broker (%v); ignore", addr, cfg.BootstrapBrokers))
		return
	}

	// Confirm that the broker really went down?
	down := false
	l.Debug("another attempt at connecting...")
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err != nil && checkErr(err) {
		l.Error("connection failed", "err", err)
		down = true
	} else {
		conn.Close()
	}

	// reconnect with next node
	if down {
		l.Debug("cleaning up resources for old client")
		// pause current client; drops the internally buffered recs
		cl.PauseFetchTopics(cfg.Topics...)

		// exit the poll fetch loop for this consumer group
		cancel()

		// Add a retry backoff and loop through next nodes and break after few attempts
	Loop:
		for h.retries <= h.maxRetries {
			if h.retries > 0 {
				l.Debug("retrying...", "count", h.retries, "max_retries", h.maxRetries)
			}

			err := h.m.connectToNextNode()
			if err != nil {
				l.Error("error creating consumer group", "brokers", cfg.BootstrapBrokers, "err", err)
				if errors.Is(err, ErrBrokerUnavailable) {
					h.retries++
					waitTries(ctx, h.retryBackoffFn(h.retries))
				}
				continue Loop
			}

			break Loop
		}

		l.Debug("consumer group is connected now", "brokers", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
	}
}

// initConsumer initalizes the consumer when the programs boots up.
func initConsumer(ctx context.Context, m *consumerManager, cfgs []ConsumerGroupCfg, l *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	c := consumer{
		cfgs:      cfgs,
		logger:    l,
		parentCtx: ctx,
		ctx:       make([]context.Context, len(cfgs)),
		cancelFn:  make([]context.CancelFunc, len(cfgs)),
		clients:   make([]*kgo.Client, len(cfgs)),
	}

	c.ctx = append(c.ctx, ctx)
	c.cancelFn = append(c.cancelFn, cancel)

	// set consumer under manager
	m.c = &c

	// try connecting to the consumers one by one
	// exit if none of the given nodes are available.
	var (
		err        error
		defaultIdx = -1
		brokersUp  = make(map[string]struct{})
		retries    = 0
		backoff    = retryBackoff()
	)
	for i := 0; i < len(cfgs); i++ {
		l.Info("creating consumer group", "broker", cfgs[i].BootstrapBrokers, "group_id", cfgs[i].GroupID)
		if err = m.connectToNextNode(); err != nil {
			l.Error("error creating consumer", "err", err)
			if errors.Is(err, ErrBrokerUnavailable) {
				retries++
				waitTries(ctx, backoff(retries))
			}

			continue
		}

		// mark the consumer group that is up
		brokersUp[cfgs[i].GroupID] = struct{}{}

		// Note down the consumer group index to make it default
		if defaultIdx == -1 {
			defaultIdx = i
		}
	}

	brokerUp := (defaultIdx != -1)

	// return error if none of the brokers are available
	if !brokerUp && err != nil {
		return err
	}

	// set the default active consumer group
	// TODO: Close other consumer groups?
	m.setActive(defaultIdx)

	m.brokersUp = brokersUp

	return nil
}

// setActive assigns current active index inside consumer manager
func (m *consumerManager) setActive(idx int) {
	m.c.idx = idx
	m.c.nextIndex = idx + 1
}

// connect selects the the configuration (round-robin fashion) and proceeds to create
func (m *consumerManager) connectToNextNode() error {
	// select consumer config in round-robin fashion
	m.c.nodeIncr()

	var (
		cfg  = m.getCurrentConfig()
		cl   = m.getCurrentClient()
		l    = m.c.logger
		err  error
		conn net.Conn
	)

	l.Debug("attempting to connect to broker", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
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
		l.Debug("reusing consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)

		if err := leaveAndResetOffsets(ctx, cl, cfg, m.c.offsets, l); err != nil {
			l.Error("error leave and reset offsets", "err", err)
			return err
		}
	} else {
		l.Debug("creating consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
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
		l.Debug("reinitializing consumer group", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
		cl, err = m.initKafkaConsumerGroup()
		if err != nil {
			return err
		}

		l.Debug("resuming consumer fetch topics", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID, "topics", cfg.Topics)
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
