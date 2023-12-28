package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/VictoriaMetrics/metrics"
	"github.com/joeirimpan/kaf-relay/filter"
	"github.com/knadh/koanf/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// initProducer initializes the kafka producer client.
func initProducer(ctx context.Context, cfg ProducerCfg, m *metrics.Set, l *slog.Logger) (*producer, error) {
	l.Info("creating producer", "broker", cfg.BootstrapBrokers)

	prod := New(cfg, m, l)
	opts := []kgo.Opt{
		kgo.AllowAutoTopicCreation(),
		kgo.ProduceRequestTimeout(cfg.SessionTimeout),
		kgo.RecordDeliveryTimeout(cfg.SessionTimeout), // break the :ProduceSync if it takes too long
		kgo.RequestRetries(cfg.MaxRetries),
		kgo.ProducerBatchMaxBytes(int32(cfg.MaxMessageBytes)),
		kgo.MaxBufferedRecords(cfg.BatchSize),
		kgo.ProducerLinger(cfg.FlushFrequency),
		kgo.ProducerBatchCompression(getCompressionCodec(cfg.Compression)),
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.RequiredAcks(getAckPolicy(cfg.CommitAck)),
	}

	// TCPAck/LeaderAck requires kafka deduplication to be turned off
	if !cfg.EnableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

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

	var (
		retries = 0
		backoff = retryBackoff()
		err     error
		cl      *kgo.Client
	)

	// retry until we can connect to kafka
outerLoop:
	for retries < cfg.MaxRetries || cfg.MaxRetries == IndefiniteRetry {
		select {
		case <-ctx.Done():
			break outerLoop
		default:
			cl, err = kgo.NewClient(opts...)
			if err != nil {
				l.Error("error creating producer client", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}
			prod.SetClient(cl)

			// Get the destination topics
			var topics []string
			for _, v := range cfg.Topics {
				topics = append(topics, v)
			}

			// test connectivity and ensures destination topics exists.
			err = testConnection(cl, cfg.SessionTimeout, topics)
			if err != nil {
				l.Error("error connecting to producer", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			if err == nil {
				break outerLoop
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return prod, nil
}

// initConsumerGroup initializes a Kafka consumer group.
func (c *consumer) initConsumerGroup(ctx context.Context, cfg ConsumerGroupCfg) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(childCtx context.Context, cl *kgo.Client, claims map[string][]int32) {
			select {
			case <-ctx.Done():
				return
			case <-childCtx.Done():
				return
			default:
				c.l.Debug("partitioned assigned", "broker", cl.OptValue(kgo.SeedBrokers), "claims", claims)
			}
		}),
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

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if err := testConnection(cl, cfg.SessionTimeout, cfg.Topics); err != nil {
		return nil, err
	}

	return cl, nil
}

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
