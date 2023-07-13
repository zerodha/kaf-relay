package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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
			Offset: r.Offset + 1,
		}
		tOMap := make(map[string]map[int32]kgo.EpochOffset)
		tOMap[r.Topic] = oMap
		c.client.CommitOffsetsSync(ctx, tOMap, nil)
		return
	}

	c.client.MarkCommitRecords(r)
}

func initConsumer(ctx context.Context, cfg ConsumerGroupConfig) (consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, m map[string][]int32) {
			// on close triggers this callback, use this to commit the marked offsets before exiting.
			nCtx, cancel := context.WithTimeout(ctx, cfg.SessionTimeout)
			defer cancel()

			if err := c.CommitMarkedOffsets(nCtx); err != nil {
				log.Printf("error committing marked offsets: %v", err)
			}
		}),
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

	// Add authentication
	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientConfig)
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
	client *kgo.Client
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

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	// Add authentication
	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientConfig)
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

func appendSASL(opts []kgo.Opt, cfg ClientConfig) []kgo.Opt {
	switch m := cfg.SASLMechanism; m {
	case SASLMechanismPlain:
		p := plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}
		opts = append(opts, kgo.SASL(p.AsMechanism()))

	case SASLMechanismScramSHA256, SASLMechanismScramSHA512:
		p := scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}

		mech := p.AsSha256Mechanism()
		if m == SASLMechanismScramSHA512 {
			mech = p.AsSha512Mechanism()
		}

		opts = append(opts, kgo.SASL(mech))
	}

	return opts
}
