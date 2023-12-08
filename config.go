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
	App AppCfg `koanf:"app"`

	Consumers []ConsumerGroupCfg `koanf:"consumers"`
	Producer  ProducerCfg        `koanf:"producer"`
	Topics    map[string]string  `koanf:"topics"`
}

// AppCfg is other miscellaneous app configs.
type AppCfg struct {
	MaxFailovers int `koanf:"max_failovers"`

	Checkpoint string `koanf:"checkpoint"`

	LogLevel          slog.Level `koanf:"log_level"`
	MetricsServerAddr string     `koanf:"metrics_server_addr"`

	LagThreshold   int64         `koanf:"lag_threshold"`
	LagMonitorFreq time.Duration `koanf:"lag_monitor_frequency"`
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

	GroupID              string        `koanf:"group_id"`
	Offset               string        `koanf:"offset"` // start/end
	MaxWaitTime          time.Duration `koanf:"max_wait_time"`
	OffsetCommitInterval time.Duration `koanf:"offset_commit_interval"`

	MaxFailovers int `koanf:"max_failovers"`
	Topics       []string
}

// ProducerCfg is the producer specific config.
type ProducerCfg struct {
	ClientCfg `koanf:",squash"`

	// disabling idempotent produce requests allows TCPAck/LeaderAck
	EnableIdempotency bool `koanf:"enable_idempotency"`
	// required acks
	CommitAck      string        `koanf:"commit_ack_type"` // tcp/leader/cluster/default
	MaxRetries     int           `koanf:"max_retries"`
	FlushFrequency time.Duration `koanf:"flush_frequency"`
	// Upper bound of max message bytes to produce
	MaxMessageBytes int `koanf:"max_message_bytes"`
	// Buffer produce messages
	BatchSize int `koanf:"batch_size"`
	// compression
	Compression string `koanf:"compression"` // gzip/snappy/lz4/zstd/none

	Topics map[string]string
}
