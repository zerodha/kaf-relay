package main

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type producer struct {
	client *kgo.Client
	cfg    ProducerCfg

	logger *slog.Logger
}

func initProducer(ctx context.Context, cfg ProducerCfg, l *slog.Logger) (*producer, error) {
	l.Info("creating producer", "broker", cfg.BootstrapBrokers)

	prod := &producer{logger: l}
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
	)

outerLoop:
	for retries < cfg.MaxRetries || cfg.MaxRetries == IndefiniteRetry {
		select {
		case <-ctx.Done():
			break outerLoop
		default:
			prod.client, err = kgo.NewClient(opts...)
			if err != nil {
				l.Error("error creating producer client", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			// Get the destination topics
			var topics []string
			for _, v := range cfg.Topics {
				topics = append(topics, v)
			}

			// test connectivity and ensures destination topics exists.
			err = testConnection(prod.client, cfg.SessionTimeout, topics)
			if err != nil {
				l.Error("error connecting to producer", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			if err == nil {
				prod.cfg = cfg
				break outerLoop
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return prod, nil
}
