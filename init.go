package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"plugin"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	flag "github.com/spf13/pflag"
	"github.com/zerodha/kaf-relay/filter"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

var cliFlags *flag.FlagSet

func initFlags(ko *koanf.Koanf) {
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println(f.FlagUsages())
		os.Exit(0)
	}

	f.StringSlice("config", []string{"config.toml"}, "path to one or more config files (will be merged in order)")
	f.String("mode", "single", "single | failover")
	f.Bool("stop-at-end", false, "stop relay at the end of offsets")
	f.StringSlice("filter", []string{}, "path to one or more filter providers")
	f.StringSlice("topic", []string{}, "one or more source:target topic names. Setting this overrides [topics] in the config file.")
	f.Bool("version", false, "show current version of the build")

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Fatalf("error loading flags: %v", err)
	}
	cliFlags = f

	if err := ko.Load(posflag.Provider(f, ".", ko), nil); err != nil {
		log.Fatalf("error reading flag config: %v", err)
	}
}

func initConfig(ko *koanf.Koanf) {
	// Load one or more config files. Keys in each subsequent file is merged
	// into the previous file's keys.
	for _, f := range ko.Strings("config") {
		log.Printf("reading config from %s", f)
		if err := ko.Load(file.Provider(f), toml.Parser()); err != nil {
			log.Fatalf("error reading config: %v", err)
		}
	}

	// Re-apply only explicitly supplied flags after loading configuration files.
	// This preserves the expected CLI-over-file precedence without allowing flag
	// defaults to overwrite values from the configuration.
	if cliFlags != nil {
		if err := ko.Load(posflag.Provider(cliFlags, ".", ko), nil); err != nil {
			log.Fatalf("error applying flag overrides: %v", err)
		}
	}

	if err := validateConfig(ko); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
}

func validateConfig(ko *koanf.Koanf) error {
	mode := ko.String("mode")
	if mode != relay.ModeSingle && mode != relay.ModeFailover {
		return fmt.Errorf("mode must be %q or %q, got %q", relay.ModeSingle, relay.ModeFailover, mode)
	}
	if ko.Bool("stop-at-end") && mode == relay.ModeFailover {
		return fmt.Errorf("`--stop-at-end` cannot be used with `failover` mode")
	}

	if ko.String("app.metrics_server_addr") == "" {
		return fmt.Errorf("app.metrics_server_addr must not be empty")
	}
	if logLevel := ko.String("app.log_level"); logLevel == "" {
		return fmt.Errorf("app.log_level must not be empty")
	} else {
		var level slog.Level
		if err := json.Unmarshal([]byte(fmt.Sprintf(`%q`, logLevel)), &level); err != nil {
			return fmt.Errorf("app.log_level: %w", err)
		}
	}

	if err := validateTopicConfig(ko); err != nil {
		return err
	}

	if d := ko.Duration("source_pool.healthcheck_interval"); d <= 0 {
		return fmt.Errorf("source_pool.healthcheck_interval must be positive")
	}
	if d := ko.Duration("source_pool.request_timeout"); d <= 0 {
		return fmt.Errorf("source_pool.request_timeout must be positive")
	}
	if ko.Int64("source_pool.offset_lag_threshold") < 0 {
		return fmt.Errorf("source_pool.offset_lag_threshold must not be negative")
	}
	if err := validateRetryConfig("source_pool", ko.Int("source_pool.max_retries"), ko.Bool("source_pool.backoff_enable"), ko.Duration("source_pool.backoff_min"), ko.Duration("source_pool.backoff_max")); err != nil {
		return err
	}

	var src struct {
		Sources []relay.ConsumerCfg `koanf:"sources"`
	}
	if err := ko.Unmarshal("", &src); err != nil {
		return fmt.Errorf("sources: %w", err)
	}
	if len(src.Sources) == 0 {
		return fmt.Errorf("at least one source must be configured")
	}
	for i, source := range src.Sources {
		if err := validateKafkaConfig(fmt.Sprintf("sources[%d]", i), source.KafkaCfg); err != nil {
			return err
		}
	}

	var target relay.ProducerCfg
	if err := ko.Unmarshal("target", &target); err != nil {
		return fmt.Errorf("target: %w", err)
	}
	if err := validateKafkaConfig("target", target.KafkaCfg); err != nil {
		return err
	}
	if err := validateRetryConfig("target", target.MaxRetries, ko.Bool("target.backoff_enable"), ko.Duration("target.backoff_min"), ko.Duration("target.backoff_max")); err != nil {
		return err
	}
	if ko.Duration("target.request_timeout") <= 0 {
		return fmt.Errorf("target.request_timeout must be positive")
	}
	if target.FlushFrequency <= 0 {
		return fmt.Errorf("target.flush_frequency must be positive")
	}
	if target.FlushBatchSize <= 0 {
		return fmt.Errorf("target.flush_batch_size must be positive")
	}
	if target.BatchSize <= 0 {
		return fmt.Errorf("target.batch_size must be positive")
	}
	if target.BufferSize <= 0 {
		return fmt.Errorf("target.buffer_size must be positive")
	}
	if target.MaxMessageBytes <= 0 {
		return fmt.Errorf("target.max_message_bytes must be positive")
	}
	if !validAckPolicy(target.CommitAck) {
		return fmt.Errorf("target.commit_ack_type has unsupported value %q", target.CommitAck)
	}
	if !validCompression(target.Compression) {
		return fmt.Errorf("target.compression has unsupported value %q", target.Compression)
	}

	return nil
}

func validateTopicConfig(ko *koanf.Koanf) error {
	if topicNames := ko.Strings("topic"); len(topicNames) > 0 {
		for _, topic := range topicNames {
			if _, err := parseTopicExpression(topic); err != nil {
				return err
			}
		}
		return nil
	}

	var configured map[string]string
	if err := ko.Unmarshal("topics", &configured); err != nil {
		return fmt.Errorf("topics: %w", err)
	}

	if len(configured) == 0 && len(ko.Strings("topic")) == 0 {
		return fmt.Errorf("no topic map specified")
	}
	for source, target := range configured {
		if source == "" || target == "" {
			return fmt.Errorf("topic source and target must not be empty")
		}
		if err := validateTopicTarget(target); err != nil {
			return fmt.Errorf("topics.%s: %w", source, err)
		}
	}
	return nil
}

func validateTopicTarget(target string) error {
	split := strings.Split(target, ":")
	if len(split) > 2 || split[0] == "" {
		return fmt.Errorf("target must be topic or topic:partition")
	}
	if len(split) == 2 {
		if _, err := parsePartition(split[1]); err != nil {
			return fmt.Errorf("partition must be a non-negative integer")
		}
	}
	return nil
}

func parsePartition(value string) (int, error) {
	partition, err := strconv.ParseInt(value, 10, 32)
	if err != nil || partition < 0 {
		return 0, fmt.Errorf("partition must be a non-negative 32-bit integer")
	}
	return int(partition), nil
}

func validateRetryConfig(name string, maxRetries int, backoffEnabled bool, min, max time.Duration) error {
	if maxRetries < relay.IndefiniteRetry {
		return fmt.Errorf("%s.max_retries must be -1 or greater", name)
	}
	if !backoffEnabled {
		return nil
	}
	if min <= 0 || max <= 0 {
		return fmt.Errorf("%s.backoff_min and %s.backoff_max must be positive", name, name)
	}
	if min > max {
		return fmt.Errorf("%s.backoff_min must not exceed %s.backoff_max", name, name)
	}
	return nil
}

func validateKafkaConfig(name string, cfg relay.KafkaCfg) error {
	if len(cfg.BootstrapBrokers) == 0 {
		return fmt.Errorf("%s.servers must contain at least one broker", name)
	}
	if cfg.SessionTimeout <= 0 {
		return fmt.Errorf("%s.session_timeout must be positive", name)
	}
	if cfg.EnableAuth {
		if !validSASLMechanism(cfg.SASLMechanism) {
			return fmt.Errorf("%s.sasl_mechanism has unsupported value %q", name, cfg.SASLMechanism)
		}
		if cfg.Username == "" || cfg.Password == "" {
			return fmt.Errorf("%s.username and %s.password are required when authentication is enabled", name, name)
		}
	}
	certPaths := []string{cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath}
	providedCerts := 0
	for _, path := range certPaths {
		if path != "" {
			providedCerts++
		}
	}
	if cfg.EnableTLS && providedCerts != 0 && providedCerts != len(certPaths) {
		return fmt.Errorf("%s TLS certificate paths must be all provided or all empty", name)
	}
	return nil
}

func validSASLMechanism(mechanism string) bool {
	return mechanism == relay.SASLMechanismPlain || mechanism == relay.SASLMechanismScramSHA256 || mechanism == relay.SASLMechanismScramSHA512
}

func validAckPolicy(policy string) bool {
	return policy == "" || policy == "default" || policy == "tcp" || policy == "leader" || policy == "cluster"
}

func validCompression(compression string) bool {
	switch compression {
	case "", "gzip", "snappy", "lz4", "zstd", "none":
		return true
	default:
		return false
	}
}

func initLog(ko *koanf.Koanf) *slog.Logger {
	var logLevel slog.Level
	if err := json.Unmarshal([]byte(fmt.Sprintf(`"%s"`, ko.MustString("app.log_level"))), &logLevel); err != nil {
		log.Fatalf("error unmarshalling log level: %v", err)
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     logLevel,
	}))
}

func initTargetConfig(ko *koanf.Koanf) relay.TargetCfg {
	return relay.TargetCfg{
		ReqTimeout:    ko.MustDuration("target.request_timeout"),
		EnableBackoff: ko.Bool("target.backoff_enable"),
		BackoffMin:    ko.MustDuration("target.backoff_min"),
		BackoffMax:    ko.MustDuration("target.backoff_max"),
	}
}

func initSourcePoolConfig(ko *koanf.Koanf) relay.SourcePoolCfg {
	return relay.SourcePoolCfg{
		HealthCheckInterval: ko.MustDuration("source_pool.healthcheck_interval"),
		ReqTimeout:          ko.MustDuration("source_pool.request_timeout"),
		LagThreshold:        ko.MustInt64("source_pool.offset_lag_threshold"),
		MaxRetries:          ko.MustInt("source_pool.max_retries"),
		EnableBackoff:       ko.Bool("source_pool.backoff_enable"),
		BackoffMin:          ko.MustDuration("source_pool.backoff_min"),
		BackoffMax:          ko.MustDuration("source_pool.backoff_max"),
	}
}

func initRelayConfig(ko *koanf.Koanf) relay.RelayCfg {
	return relay.RelayCfg{
		StopAtEnd: ko.Bool("stop-at-end"),
	}
}

// initTopicsMap parses the topic map from the [[topics]] config in
// the config file and --topic cli flag.
func initTopicsMap(ko *koanf.Koanf) relay.Topics {
	if topicNames := ko.Strings("topic"); len(topicNames) > 0 {
		out := relay.Topics{}
		for _, topicName := range topicNames {
			topic, err := parseTopicExpression(topicName)
			if err != nil {
				log.Fatal(err)
			}
			out[topic.SourceTopic] = topic
		}
		return out
	}

	var mp map[string]string
	if err := ko.Unmarshal("topics", &mp); err != nil {
		log.Fatalf("error marshalling `topics` config: %v", err)
	}

	out := relay.Topics{}
	for src, target := range mp {
		var (
			autoPartition = true
			partition     int
		)

		// The target topic value is in the format topic:parititon.
		if split := strings.Split(target, ":"); len(split) == 2 {
			target = split[0]
			autoPartition = false

			p, err := parsePartition(split[1])
			if err != nil {
				log.Fatalf("invalid topic:partition config in '%s'", target)
			}

			partition = p
		}

		out[src] = relay.Topic{
			SourceTopic:         src,
			TargetTopic:         target,
			TargetPartition:     uint(partition),
			AutoTargetPartition: autoPartition,
		}
	}

	if len(out) == 0 {
		log.Fatalf("no topic map specified")
	}

	return out
}

func parseTopicExpression(expression string) (relay.Topic, error) {
	split := strings.Split(expression, ":")
	if len(split) != 2 && len(split) != 3 {
		return relay.Topic{}, fmt.Errorf("invalid topic %q: use source:target or source:target:partition", expression)
	}
	if split[0] == "" || split[1] == "" {
		return relay.Topic{}, fmt.Errorf("invalid topic %q: source and target must not be empty", expression)
	}

	topic := relay.Topic{
		SourceTopic:         split[0],
		TargetTopic:         split[1],
		AutoTargetPartition: len(split) == 2,
	}
	if len(split) == 3 {
		partition, err := parsePartition(split[2])
		if err != nil {
			return relay.Topic{}, fmt.Errorf("invalid topic %q: %w", expression, err)
		}
		topic.TargetPartition = uint(partition)
	}
	return topic, nil
}

// initKafkaConfig reads the source(s)/target Kafka configuration.
func initKafkaConfig(ko *koanf.Koanf) ([]relay.ConsumerCfg, relay.ProducerCfg) {
	// Read source Kafka config.
	src := struct {
		Sources []relay.ConsumerCfg `koanf:"sources"`
	}{}

	if err := ko.Unmarshal("", &src); err != nil {
		log.Fatalf("error unmarshalling `sources` config: %v", err)
	}

	log.Printf("read config for %d servers in the source pool", len(src.Sources))
	if ko.Bool("source_pool.randomize_initial") {
		log.Println("randomizing source pool for initial connection")
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Shuffle(len(src.Sources), func(i, j int) {
			src.Sources[i], src.Sources[j] = src.Sources[j], src.Sources[i]
		})
	}

	// If it's single mode, eliminate all servers in the pool except one
	// to disable healthcehcks and failover.
	if ko.String("mode") == relay.ModeSingle {
		src.Sources = src.Sources[:1]
	}

	// Read target Kafka config.
	var prod relay.ProducerCfg
	if err := ko.Unmarshal("target", &prod); err != nil {
		log.Fatalf("error unmarshalling `sources` config: %v", err)
	}

	return src.Sources, prod
}

// initFilters loads the go plugin, initializes it and return a map of filter plugins.
func initFilters(ko *koanf.Koanf, lo *slog.Logger) (map[string]filter.Provider, error) {
	filterIDs := ko.MapKeys("filters")
	filterPaths := ko.Strings("filter")
	enabledConfiguredFilters := false
	for _, id := range filterIDs {
		if ko.Bool("filters." + id + ".enabled") {
			enabledConfiguredFilters = true
			break
		}
	}
	if ko.String("mode") != relay.ModeSingle && (enabledConfiguredFilters || len(filterPaths) > 0) {
		return nil, fmt.Errorf("filters can only be used in %q mode", relay.ModeSingle)
	}

	out := make(map[string]filter.Provider)
	sort.Strings(filterIDs)
	for _, id := range filterIDs {
		if !ko.Bool("filters." + id + ".enabled") {
			continue
		}

		path := ko.String("filters." + id + ".path")
		var cfg filter.Config
		if err := ko.Unmarshal("filters."+id, &cfg); err != nil {
			return nil, fmt.Errorf("error unmarshalling filter config %q: %w", id, err)
		}
		if cfg.Config == "" {
			lo.Warn("filter has no configuration", "id", id)
		}

		p, err := loadFilterProvider(id, path, []byte(cfg.Config), lo, true)
		if err != nil {
			return nil, err
		}
		out[id] = p
	}

	for _, path := range filterPaths {
		// CLI-selected plugins do not have a separate config flag yet. Pass an
		// empty JSON object so providers that decode JSON can still initialize.
		p, err := loadFilterProvider("", path, []byte("{}"), lo, false)
		if err != nil {
			return nil, err
		}
		if _, exists := out[p.ID()]; exists {
			return nil, fmt.Errorf("duplicate filter provider ID %q", p.ID())
		}
		out[p.ID()] = p
	}

	return out, nil
}

func loadFilterProvider(expectedID, path string, config []byte, lo *slog.Logger, enforceID bool) (filter.Provider, error) {
	plg, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error loading provider plugin %q from %q: %w", expectedID, path, err)
	}

	newFunc, err := plg.Lookup("New")
	if err != nil {
		return nil, fmt.Errorf("New() function not found in plugin %q: %w", expectedID, err)
	}
	f, ok := newFunc.(func([]byte) (interface{}, error))
	if !ok {
		return nil, fmt.Errorf("New() function has invalid type %T in plugin %q", newFunc, expectedID)
	}

	provider, err := f(config)
	if err != nil {
		return nil, fmt.Errorf("error initializing filter provider plugin %q: %w", expectedID, err)
	}
	p, ok := provider.(filter.Provider)
	if !ok {
		return nil, fmt.Errorf("New() function does not return filter.Provider (%T) in plugin %q", provider, expectedID)
	}
	if enforceID && p.ID() != expectedID {
		return nil, fmt.Errorf("filter provider plugin ID does not match %q != %q", expectedID, p.ID())
	}

	lo.Info("loaded filter provider plugin", "id", p.ID(), "path", path)
	return p, nil
}

func initMetricsServer(metrics *metrics.Set, ko *koanf.Koanf) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		buf := &bytes.Buffer{}
		metrics.WritePrometheus(buf)

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		buf.WriteTo(w)
	})

	return &http.Server{
		Addr:         ko.MustString("app.metrics_server_addr"),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}
