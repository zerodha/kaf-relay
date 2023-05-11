package main

import "time"

type Config struct {
	Consumer ConsumerGroupConfig `koanf:"consumer"`
	Producer ProducerConfig      `koanf:"producer"`
	Topics   map[string]string   `koanf:"topics"`
}

// ClientConfig is the message broker's client config.
type ClientConfig struct {
	// Broker.
	BootstrapBrokers []string      `koanf:"servers"`
	SessionTimeout   time.Duration `koanf:"session_timeout"`

	// Auth.
	Username   string `koanf:"username"`
	Password   string `koanf:"password"`
	EnableAuth bool   `koanf:"auth_enabled"`
}

// ConsumerGroupConfig is the consumer group specific config.
type ConsumerGroupConfig struct {
	ClientConfig `koanf:"client"`

	GroupID              string        `koanf:"group_id"`
	Offset               string        `koanf:"offset"` // start/end
	MaxWaitTime          time.Duration `koanf:"max_wait_time"`
	OffsetCommitInterval time.Duration `koanf:"offset_commit_interval"`
	Topics               []string
}

// ProducerConfig is the producer specific config.
type ProducerConfig struct {
	ClientConfig `koanf:"client"`

	// disabling idempotent produce requests allows TCPAck/LeaderAck
	EnableIdempotency bool `koanf:"enable_idempotency"`
	// required acks
	CommitAck           string        `koanf:"commit_ack_type"`    // tcp/leader/cluster/default
	PartitionerStrategy string        `koanf:"partition_strategy"` // manual
	MaxRetries          int           `koanf:"max_retries"`
	FlushFrequency      time.Duration `koanf:"flush_frequency"`
	// Upper bound of max message bytes to produce
	MaxMessageBytes int `koanf:"max_message_bytes"`
	// Buffer produce messages
	BatchSize int `koanf:"batch_size"`
	// compression
	Compression string `koanf:"compression"` // gzip/snappy/lz4/zstd/none

	Topics map[string]string
}
