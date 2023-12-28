package main

import (
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
		h.m.c.logger.Info("reconnect in progress; ignore OnBrokerDisconnect callback")
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
	if !inSlice(addr, cfg.BootstrapBrokers) || cl == nil {
		l.Debug(fmt.Sprintf("%s is not current active broker (%v); ignore", addr, cfg.BootstrapBrokers))
		return
	}

	// reconnect with next node
	l.Debug("cleaning up resources for old client")
	// pause current client; drops the internally buffered recs
	cl.PauseFetchTopics(cfg.Topics...)

	// exit the poll fetch loop for this consumer group
	cancel()

	// Add a retry backoff and loop through next nodes and break after few attempts
Loop:
	for h.retries <= h.maxRetries || h.maxRetries == IndefiniteRetry {
		// check for context cancellations
		select {
		case <-h.m.c.parentCtx.Done():
			return
		default:
		}

		l.Info("connecting to node...", "count", h.retries, "max_retries", h.maxRetries)

		err := h.m.connectToNextNode()
		if err != nil {
			cfg := h.m.getCurrentConfig()
			l.Error("error creating consumer group", "brokers", cfg.BootstrapBrokers, "err", err)
			h.retries++

			waitTries(h.m.c.parentCtx, h.retryBackoffFn(h.retries))

			continue Loop
		}

		break
	}

	cfg = h.m.getCurrentConfig()
	l.Info("failover successful; consumer group is connected now", "brokers", cfg.BootstrapBrokers, "group_id", cfg.GroupID)
}
