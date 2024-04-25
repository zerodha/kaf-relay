package main

import (
	"log/slog"
	"time"
)

const (
	SASLMechanismPlain       = "PLAIN"
	SASLMechanismScramSHA256 = "SCRAM-SHA-256"
	SASLMechanismScramSHA512 = "SCRAM-SHA-512"
)

// Config is a holder struct for all configs.
type Config struct {
	App struct {
		LogLevel            slog.Level    `koanf:"log_level"`
		MaxFailovers        int           `koanf:"max_failovers"`
		MaxRequestDuration  time.Duration `koanf:"max_request_duration"`
		MetricsServerAddr   string        `koanf:"metrics_server_addr"`
		LagThreshold        int64         `koanf:"lag_threshold"`
		NodeHealthCheckFreq time.Duration `koanf:"node_health_check_frequency"`
		Backoff             BackoffCfg    `koanf:"retry_backoff"`
	} `koanf:"app"`

	Sources   []ConsumerGroupCfg `koanf:"sources"`
	Target    ProducerCfg        `koanf:"target"`
	TopicsMap map[string]string  `koanf:"topics"`

	// source_topic:TopicConfig map.
	// This is post-processed and derived from the simple string TopicsMap.
	Topics map[string]Topic `koanf:"-"`
}

type Topic struct {
	SourceTopic         string
	TargetTopic         string
	TargetPartition     uint
	AutoTargetPartition bool
}

// BackoffCfg is the retry backoff config.
type BackoffCfg struct {
	Enable bool          `koanf:"enable"`
	Min    time.Duration `koanf:"min"`
	Max    time.Duration `koanf:"max"`
}

// ClientCfg is the message broker's client config.
type ClientCfg struct {
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
	ClientCfg `koanf:",squash"`

	GroupID     string        `koanf:"group_id"`
	InstanceID  string        `koanf:"instance_id"`
	MaxWaitTime time.Duration `koanf:"max_wait_time"`

	MaxFailovers int `koanf:"max_failovers"`
	Topics       []string
}

// ProducerCfg is the producer specific config.
type ProducerCfg struct {
	ClientCfg `koanf:",squash"`

	EnableIdempotency bool          `koanf:"enable_idempotency"`
	CommitAck         string        `koanf:"commit_ack_type"` // tcp|leader|cluster|default
	MaxRetries        int           `koanf:"max_retries"`
	FlushFrequency    time.Duration `koanf:"flush_frequency"`
	MaxMessageBytes   int           `koanf:"max_message_bytes"`
	BatchSize         int           `koanf:"batch_size"`
	FlushBatchSize    int           `koanf:"flush_batch_size"`
	Compression       string        `koanf:"compression"` // gzip|snappy|lz4|zstd|none
}
