package main

import (
	"context"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (m *consumerManager) initKafkaConsumerGroup() (*kgo.Client, error) {
	var (
		cfg      = m.getCurrentConfig()
		clCtx, _ = m.getCurrentContext()
		hook     = &consumerHook{
			m: m,

			retryBackoffFn: retryBackoff(),
			maxRetries:     cfg.MaxFailovers,
		}
		l = m.c.logger
	)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, claims map[string][]int32) {
			select {
			case <-clCtx.Done():
				return
			case <-m.c.parentCtx.Done():
				return
			case <-ctx.Done():
				return
			default:
				l.Debug("partitioned assigned", "broker", cl.OptValue(kgo.SeedBrokers), "claims", claims)
			}
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, m map[string][]int32) {
			// on close triggers this callback, use this to commit the marked offsets before exiting.
			nCtx, cancel := context.WithTimeout(ctx, cfg.SessionTimeout)
			defer cancel()

			if err := cl.CommitMarkedOffsets(nCtx); err != nil {
				l.Error("error committing marked offsets", "err", err)
			}
		}),
	}

	// OnBrokerDisconnect hook will only be active in failover mode
	if m.mode == ModeFailover {
		opts = append(opts, kgo.WithHooks(hook))
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	switch cfg.Offset {
	case "start":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	case "end":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	switch {
	case cfg.OffsetCommitInterval == 0:
		opts = append(opts, kgo.DisableAutoCommit())
	case cfg.OffsetCommitInterval > 0:
		// XXX Disable autocommit for failovers as we are not able exit autocommit goroutines safely.
		if len(m.c.cfgs) > 1 {
			l.Info("disabling autocommit for consumers failover mode")
			opts = append(opts, kgo.DisableAutoCommit())
		} else {
			opts = append(opts, kgo.AutoCommitInterval(cfg.OffsetCommitInterval))
			opts = append(opts, kgo.AutoCommitMarks())
		}
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

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return cl, nil
}
