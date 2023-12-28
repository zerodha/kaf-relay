package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrClientUninitialized = errors.New("client uninitialized")
	ErrorNoHealthy         = errors.New("no healthy node")
)

// consumer represents the kafka consumer client.
type consumer struct {
	client *kgo.Client
	cfgs   []ConsumerGroupCfg

	offsetMgr   *offsetManager
	nodeTracker *NodeTracker

	l *slog.Logger
}

// Close closes the kafka client.
func (c *consumer) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

func (c *consumer) GetHealthy(ctx context.Context) (int, error) {
	n := c.nodeTracker.GetHealthy()
	if n.Weight == -1 || n.Down {
		return -1, ErrorNoHealthy
	}

	return n.ID, nil
}

// reinit reinitializes the consumer group
func (c *consumer) Connect(ctx context.Context, cfg ConsumerGroupCfg) error {
	backoff := retryBackoff()

	// try closing existing client connection?
	if c.client != nil {
		c.client.Close()
	}

	for retryCount := 0; retryCount < 3; retryCount++ {
		c.l.Debug("reinitializing consumer group", "broker", cfg.BootstrapBrokers, "retries", retryCount)

		cl, err := c.initConsumerGroup(ctx, cfg)
		if err != nil {
			return err
		}

		offsets := c.offsetMgr.Get()
		if offsets != nil {
			err = leaveAndResetOffsets(ctx, cl, cfg, offsets, c.l)
			if err != nil {
				c.l.Error("error resetting offsets", "err", err)
				if errors.Is(err, ErrLaggingBehind) {

					return err
				}

				waitTries(ctx, backoff(retryCount))
				continue
			}

			cl, err = c.initConsumerGroup(ctx, cfg)
			if err != nil {
				return err
			}
		}

		c.client = cl
		break
	}

	return nil
}
