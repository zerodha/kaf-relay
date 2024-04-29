package relay

import (
	"time"
)

// Topic represents a source->target topic configuration.
type Topic struct {
	SourceTopic         string
	TargetTopic         string
	TargetPartition     uint
	AutoTargetPartition bool
}

// KafkaCfg is the message broker's client config.
type KafkaCfg struct {
	// Namespace
	Name string `koanf:"name"`

	// Broker.
	BootstrapBrokers []string      `koanf:"servers"`
	SessionTimeout   time.Duration `koanf:"session_timeout"`

	// Auth.
	EnableAuth bool `koanf:"enable_auth"`
	// PLAIN/SCRAM-SHA-256/SCRAM-SHA-512
	SASLMechanism string `koanf:"sasl_mechanism"`
	Username      string `koanf:"username"`
	Password      string `koanf:"password"`

	// If enabled and the three files are passed, will
	// use the relevant certs and keys. If enabled but all three
	// file paths are empty, it will default to using DialTLS()
	EnableTLS      bool   `koanf:"enable_tls"`
	ClientKeyPath  string `koanf:"client_key_path"`
	ClientCertPath string `koanf:"client_cert_path"`
	CACertPath     string `koanf:"ca_cert_path"`

	EnableLog bool `koanf:"enable_log"`
}

// ConsumerGroupCfg is the consumer group specific config.
type ConsumerGroupCfg struct {
	KafkaCfg `koanf:",squash"`

	Topics []string
}

// ProducerCfg is the Kafka producer config.
type ProducerCfg struct {
	KafkaCfg `koanf:",squash"`

	EnableIdempotency bool          `koanf:"enable_idempotency"`
	CommitAck         string        `koanf:"commit_ack_type"` // tcp|leader|cluster|default
	MaxRetries        int           `koanf:"max_retries"`
	FlushFrequency    time.Duration `koanf:"flush_frequency"`
	MaxMessageBytes   int           `koanf:"max_message_bytes"`
	BatchSize         int           `koanf:"batch_size"`
	FlushBatchSize    int           `koanf:"flush_batch_size"`
	Compression       string        `koanf:"compression"` // gzip|snappy|lz4|zstd|none
}
