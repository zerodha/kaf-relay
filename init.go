package main

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type consumer struct {
	client *kgo.Client

	cfg ConsumerGroupConfig
}

func (c *consumer) commit(ctx context.Context, r *kgo.Record) {
	// If autocommit is disabled allow committing directly,
	// or else just mark the message to commit.
	if c.cfg.OffsetCommitInterval == 0 {
		oMap := make(map[int32]kgo.EpochOffset)
		oMap[r.Partition] = kgo.EpochOffset{
			Epoch:  r.LeaderEpoch,
			Offset: r.Offset,
		}
		tOMap := make(map[string]map[int32]kgo.EpochOffset)
		tOMap[r.Topic] = oMap
		c.client.CommitOffsets(ctx, tOMap, nil)
		return
	}

	c.client.MarkCommitRecords(r)
}

func initConsumer(cfg ConsumerGroupConfig) (consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.SessionTimeout(cfg.SessionTimeout),
	}

	// TODO: Add relative offsets
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
		opts = append(opts, kgo.AutoCommitInterval(cfg.OffsetCommitInterval))
		opts = append(opts, kgo.AutoCommitMarks())
	}

	// TODO: Add other auths
	if cfg.EnableAuth {
		p := plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}
		sasl := kgo.SASL(p.AsMechanism())
		opts = append(opts, sasl)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return consumer{}, err
	}

	// test connectivity and ensures the source topics exists
	if err := testConnection(client, cfg.SessionTimeout, cfg.Topics); err != nil {
		return consumer{}, err
	}

	return consumer{client: client, cfg: cfg}, nil
}

type producer struct {
	manualBalancer bool
	client         *kgo.Client
}

func initProducer(cfg ProducerConfig) (producer, error) {
	prod := producer{}
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

	switch cfg.PartitionerStrategy {
	case "manual":
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
		prod.manualBalancer = true
	}

	if cfg.EnableAuth {
		p := plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}
		sasl := kgo.SASL(p.AsMechanism())
		opts = append(opts, sasl)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return producer{}, err
	}

	// Get the destination topics
	var topics []string
	for _, v := range cfg.Topics {
		topics = append(topics, v)
	}

	// test connectivity and ensures destination topics exists.
	if err := testConnection(client, cfg.SessionTimeout, topics); err != nil {
		return producer{}, err
	}

	prod.client = client
	return prod, nil
}
