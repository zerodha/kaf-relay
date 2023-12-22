package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/joeirimpan/kaf-relay/filter"
	"github.com/knadh/koanf/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// initFilterProviders loads the go plugin, initializes it and return a map of filter plugins.
func initFilterProviders(names []string, ko *koanf.Koanf, log *slog.Logger) (map[string]filter.Provider, error) {
	out := make(map[string]filter.Provider)
	for _, fName := range names {
		plg, err := plugin.Open(fName)
		if err != nil {
			return nil, fmt.Errorf("error loading provider plugin '%s': %v", fName, err)
		}
		id := strings.TrimSuffix(filepath.Base(fName), filepath.Ext(fName))

		newFunc, err := plg.Lookup("New")
		if err != nil {
			return nil, fmt.Errorf("New() function not found in plugin '%s': %v", id, err)
		}
		f, ok := newFunc.(func([]byte) (interface{}, error))
		if !ok {
			return nil, fmt.Errorf("New() function is of invalid type (%T) in plugin '%s'", newFunc, id)
		}

		var cfg filter.Config
		ko.Unmarshal("filter."+id, &cfg)
		if cfg.Config == "" {
			log.Info(fmt.Sprintf("WARNING: No config 'filter.%s' for '%s' in config", id, id))
		}

		// Initialize the plugin.
		prov, err := f([]byte(cfg.Config))
		if err != nil {
			return nil, fmt.Errorf("error initializing filter provider plugin '%s': %v", id, err)
		}
		log.Info(fmt.Sprintf("loaded filter provider plugin '%s' from %s", id, fName))

		p, ok := prov.(filter.Provider)
		if !ok {
			return nil, fmt.Errorf("New() function does not return a provider that satisfies filter.Provider (%T) in plugin '%s'", prov, id)
		}

		if p.ID() != id {
			return nil, fmt.Errorf("filter provider plugin ID doesn't match '%s' != %s", id, p.ID())
		}
		out[p.ID()] = p
	}

	return out, nil
}

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
			// in failover mode this is the only time we commit all the marked records. For single mode
			// this flushes the marked records that remain (auto-committing routinely commits marked offsets)
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

	opts = append(opts, kgo.DisableAutoCommit())

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
