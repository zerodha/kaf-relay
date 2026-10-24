package main

import (
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf/v2"
)

func validConfigForTest(t *testing.T) *koanf.Koanf {
	t.Helper()

	ko := koanf.New(".")
	values := map[string]any{
		"mode":                             "single",
		"app.log_level":                    "info",
		"app.metrics_server_addr":          "127.0.0.1:7081",
		"topics.source":                    "target",
		"source_pool.healthcheck_interval": 3 * time.Second,
		"source_pool.request_timeout":      100 * time.Millisecond,
		"source_pool.offset_lag_threshold": 1000,
		"source_pool.max_retries":          -1,
		"source_pool.backoff_min":          time.Second,
		"source_pool.backoff_max":          2 * time.Second,
		"target.request_timeout":           100 * time.Millisecond,
		"target.backoff_min":               time.Second,
		"target.backoff_max":               2 * time.Second,
		"target.servers":                   []string{"127.0.0.1:9092"},
		"target.session_timeout":           6 * time.Second,
		"target.flush_frequency":           20 * time.Millisecond,
		"target.max_retries":               -1,
		"target.flush_batch_size":          100,
		"target.batch_size":                100,
		"target.buffer_size":               1000,
		"target.max_message_bytes":         1000000,
		"target.commit_ack_type":           "cluster",
		"sources": []map[string]any{{
			"servers":         []string{"127.0.0.1:9091"},
			"session_timeout": 6 * time.Second,
		}},
	}

	for key, value := range values {
		if err := ko.Set(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	return ko
}

func TestValidateConfigAcceptsValidConfiguration(t *testing.T) {
	if err := validateConfig(validConfigForTest(t)); err != nil {
		t.Fatalf("validateConfig() unexpected error: %v", err)
	}
}

func TestValidateConfigRejectsUnsupportedMode(t *testing.T) {
	ko := validConfigForTest(t)
	if err := ko.Set("mode", "unsupported"); err != nil {
		t.Fatal(err)
	}

	err := validateConfig(ko)
	if err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("validateConfig() error = %v, want mode validation error", err)
	}
}

func TestValidateConfigRejectsInvalidTargetBatchSettings(t *testing.T) {
	ko := validConfigForTest(t)
	if err := ko.Set("target.flush_batch_size", 0); err != nil {
		t.Fatal(err)
	}

	err := validateConfig(ko)
	if err == nil || !strings.Contains(err.Error(), "flush_batch_size") {
		t.Fatalf("validateConfig() error = %v, want flush_batch_size validation error", err)
	}
}

func TestValidateConfigRejectsNegativeTopicPartition(t *testing.T) {
	ko := validConfigForTest(t)
	if err := ko.Set("topics.source", "target:-1"); err != nil {
		t.Fatal(err)
	}

	err := validateConfig(ko)
	if err == nil || !strings.Contains(err.Error(), "partition") {
		t.Fatalf("validateConfig() error = %v, want partition validation error", err)
	}
}
