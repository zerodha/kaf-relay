package main

import (
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type producer struct {
	client *kgo.Client

	logger *slog.Logger
}

func initProducer(cfg ProducerCfg, l *slog.Logger) (*producer, error) {
	l.Info("creating producer", "broker", cfg.BootstrapBrokers)

	prod := &producer{logger: l}
	opts := []kgo.Opt{
		kgo.AllowAutoTopicCreation(),
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

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// Get the destination topics
	var topics []string
	for _, v := range cfg.Topics {
		topics = append(topics, v)
	}

	// test connectivity and ensures destination topics exists.
	if err := testConnection(client, cfg.SessionTimeout, topics); err != nil {
		return nil, err
	}

	prod.client = client
	return prod, nil
}
