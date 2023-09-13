package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

func getAdminClient(cfg ConsumerGroupCfg) (*kadm.Client, error) {
	client, err := getClient(cfg)
	if err != nil {
		return nil, err
	}

	return kadm.NewClient(client), nil
}

func resetOffsets(ctx context.Context, cl *kadm.Client, cfg ConsumerGroupCfg, offsets map[string]map[int32]kgo.EpochOffset, l *slog.Logger) error {
	var (
		backOff = retryBackoff()
		//maxAttempts = 10
		attempts = 0
	)

	// wait for topic lap to catch up
waitForTopicLag:
	for {
		// if attempts >= maxAttempts {
		// 	return fmt.Errorf("max attempts(%d) for fetching offsets", maxAttempts)
		// }

		l.Debug("fetching end offsets", "topics", cfg.Topics)

		// Get end offsets of the topics
		topicOffsets, err := cl.ListEndOffsets(ctx, cfg.Topics...)
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

				if o.Offset > eO.Offset {
					attempts++
					b := backOff(attempts)
					l.Debug("topic end offsets was lagging beging; waiting...", "backoff", b)

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
			of.AddOffset(t, p, o.Offset, o.Epoch)
		}
	}

	l.Debug("resetting offsets for consumer group",
		"broker", cfg.BootstrapBrokers, "group", cfg.GroupID, "offsets", of)
	resp, err := cl.CommitOffsets(ctx, cfg.GroupID, of)
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
