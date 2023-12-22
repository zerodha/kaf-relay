package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// getClient returns franz-go client with default config
func getClient(cfg ConsumerGroupCfg) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.SessionTimeout(cfg.SessionTimeout),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	// Add authentication
	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := createTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return client, err
}

// leaveAndResetOffsets leaves the current consumer group and resets its offset if given.
func leaveAndResetOffsets(ctx context.Context, cl *kgo.Client, cfg ConsumerGroupCfg, offsets map[string]map[int32]kgo.Offset, l *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// leave group; mark the group as `Empty` before attempting to reset offsets.
	l.Debug("leaving group", "group_id", cfg.GroupID)
	if err := cl.LeaveGroupContext(ctx); err != nil {
		return err
	}

	// Reset consumer group offsets using the existing offsets
	if offsets != nil {
		if err := resetOffsets(ctx, cl, cfg, offsets, l); err != nil {
			return err
		}
	}

	return nil
}

// resetOffsets resets the consumer group with the given offsets map.
// Also waits for topics to catch up to the messages in case it is lagging behind.
func resetOffsets(ctx context.Context, cl *kgo.Client, cfg ConsumerGroupCfg, offsets map[string]map[int32]kgo.Offset, l *slog.Logger) error {
	var (
		backOff     = retryBackoff()
		maxAttempts = 1 // TODO: make this configurable?
		attempts    = 0
		admCl       = kadm.NewClient(cl)
	)
	defer admCl.Close()

	// wait for topic lap to catch up
waitForTopicLag:
	for {
		if attempts >= maxAttempts {
			return fmt.Errorf("max attempts(%d) for fetching offsets", maxAttempts)
		}

		l.Debug("fetching end offsets", "topics", cfg.Topics)

		// Get end offsets of the topics
		topicOffsets, err := admCl.ListEndOffsets(ctx, cfg.Topics...)
		if err != nil {
			l.Error("error fetching offsets", "err", err)
			return err
		}

		for t, po := range offsets {
			for p, o := range po {
				eO, ok := topicOffsets.Lookup(t, p)
				// TODO:
				if !ok {
					continue
				}

				if o.EpochOffset().Offset > eO.Offset {
					attempts++
					b := backOff(attempts)
					l.Info("topic end offsets was lagging beging; waiting...", "backoff", b.Seconds())

					waitTries(ctx, b)
					continue waitForTopicLag
				}
			}
		}

		break waitForTopicLag
	}

	of := make(kadm.Offsets)
	for t, po := range offsets {
		for p, o := range po {
			// Since we don't know the leader epoch we send
			// it as -1
			of.AddOffset(t, p, o.EpochOffset().Offset, -1)
		}
	}

	l.Info("resetting offsets for consumer group",
		"broker", cfg.BootstrapBrokers, "group", cfg.GroupID, "offsets", of)
	resp, err := admCl.CommitOffsets(ctx, cfg.GroupID, of)
	if err != nil {
		l.Error("error resetting group offset", "err", err)
	}

	// check for errors in offset responses
	for _, or := range resp {
		for _, r := range or {
			if r.Err != nil {
				l.Error("error resetting group offset", "err", r.Err)
				return err
			}
		}
	}

	return nil
}
