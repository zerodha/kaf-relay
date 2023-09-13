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
	"github.com/twmb/franz-go/pkg/kmsg"
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

// getCurrentConfig returns the current consumer group config.
func (m *consumerManager) getCurrentConfig() ConsumerGroupCfg {
	return m.c.cfgs[m.c.idx]
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

// commits marks/sync commit the offsets for autocommit/non-autocommit respectively
func (m *consumerManager) commit(r *kgo.Record) {
	m.Lock()
	cfg := m.getCurrentConfig()
	ctx, _ := m.getCurrentContext()
	cl := m.getCurrentClient()
	m.Unlock()

	// If autocommit is disabled allow committing directly,
	// or else just mark the message to commit.
	if cfg.OffsetCommitInterval == 0 {
		oMap := make(map[int32]kgo.EpochOffset)
		oMap[r.Partition] = kgo.EpochOffset{
			Epoch:  r.LeaderEpoch,
			Offset: r.Offset + 1,
		}
		tOMap := make(map[string]map[int32]kgo.EpochOffset)
		tOMap[r.Topic] = oMap
		cl.CommitOffsetsSync(ctx, tOMap,
			func(cl *kgo.Client, ocr1 *kmsg.OffsetCommitRequest, ocr2 *kmsg.OffsetCommitResponse, err error) {
				// keep offsets in memory
				m.c.offsets = tOMap
			},
		)
		return
	}

	cl.MarkCommitRecords(r)
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
	if !atomic.CompareAndSwapUint32(&h.m.reconnectInProgress, 0, 1) {
		h.m.c.logger.Debug("reconnect in progress; ignore OnBrokerDisconnect callback")
		return
	}

	defer func() {
		atomic.CompareAndSwapUint32(&h.m.reconnectInProgress, 1, 0)
	}()

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
	if _, err := net.DialTimeout("tcp", addr, time.Second); err != nil && checkErr(err) {
		l.Error("connection failed", "err", err)
		down = true
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
			err := h.m.connect()
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
func initConsumer(ctx context.Context, cfgs []ConsumerGroupCfg, l *slog.Logger) (*consumerManager, error) {
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

	m := consumerManager{c: &c}

	// try connecting to the consumers one by one
	// exit if none of the given nodes are available.
	var (
		err        error
		defaultIdx = -1
	)
	for i := 0; i < len(cfgs); i++ {
		l.Info("creating consumer group", "broker", cfgs[i].BootstrapBrokers, "group_id", cfgs[i].GroupID)
		if err = m.connect(); err != nil {
			l.Error("error creating consumer", "err", err)
			continue
		}
		// Note down the consumer group index to make it default
		if defaultIdx == -1 {
			defaultIdx = i
		}
	}

	brokerUp := (defaultIdx != -1)

	// return error if none of the brokers are available
	if !brokerUp && err != nil {
		return nil, err
	}

	// set the default active consumer group
	// TODO: Close other consumer groups?
	m.setActive(defaultIdx)

	return &m, nil
}

func (m *consumerManager) setActive(idx int) {
	m.c.idx = idx
	m.c.nextIndex = idx + 1
}

// connect selects the the configuration (round-robin fashion) and proceeds to create
func (m *consumerManager) connect() error {
	// select consumer config in round-robin fashion
	m.c.nodeIncr()

	var (
		cfg = m.getCurrentConfig()
		cl  = m.getCurrentClient()
		l   = m.c.logger
		err error
	)

	l.Debug("attempting to connect to broker", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
	up := false
	for _, addr := range cfg.BootstrapBrokers {
		_, err = net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			up = true
			break
		}
	}

	// Return err if we dont find atleast 1 broker available in the bootstrap broker list
	if !up && err != nil {
		return fmt.Errorf("%w: %w", ErrBrokerUnavailable, err)
	}

	// create admin client
	admCl, err := getAdminClient(cfg)
	if err != nil {
		return err
	}
	defer admCl.Close()

	// Create a new context for this consumer group poll fetch loop
	ctx, cancel := context.WithCancel(m.c.parentCtx)
	m.setCurrentContext(ctx, cancel)

	if cl != nil {
		l.Debug("reusing consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
		// Offset reset can only be done for `Empty` state consumer groups.
		// Close marks it as `Empty` eventually -> reset offset -> re-init consumer group -> voila!
		cl.Close()

		// Reset consumer group offsets using the existing offsets
		if m.c.offsets != nil {
			if err := resetOffsets(ctx, admCl, cfg, m.c.offsets, l); err != nil {
				l.Error("error resetting offset", "err", err)
				return err
			}
		}

		l.Debug("reinitializing consumer group", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
		cl, err = m.initKafkaConsumerGroup()
		if err != nil {
			return err
		}

		l.Debug("resuming consumer fetch topics", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID, "topics", cfg.Topics)
		cl.ResumeFetchTopics(cfg.Topics...)
	} else {
		l.Debug("creating consumer", "broker", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
		cl, err = m.initKafkaConsumerGroup()
		if err != nil {
			return err
		}

		// Reset consumer group offsets using the existing offsets
		if m.c.offsets != nil {
			if err := resetOffsets(ctx, admCl, cfg, m.c.offsets, l); err != nil {
				l.Error("error resetting offset", "err", err)
				return err
			}
		}
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
