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
	"github.com/zerodha/kaf-relay/internal/relay"
)

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

	if err := ko.Load(posflag.Provider(f, ".", ko), nil); err != nil {
		log.Fatalf("error reading flag config: %v", err)
	}
}

func initConfig(ko *koanf.Koanf) {
	if ko.Bool("stop-at-end") && ko.String("mode") == relay.ModeFailover {
		log.Fatalf("`--stop-at-end` cannot be used with `failover` mode")
	}

	// Load one or more config files. Keys in each subsequent file is merged
	// into the previous file's keys.
	for _, f := range ko.Strings("config") {
		log.Printf("reading config from %s", f)
		if err := ko.Load(file.Provider(f), toml.Parser()); err != nil {
			log.Fatalf("error reading config: %v", err)
		}
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

			p, err := strconv.Atoi(split[1])
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

	// If there are topicNames in the commandline flags, override the ones read from the file.
	if topicNames := ko.Strings("topic"); len(topicNames) > 0 {
		for _, t := range topicNames {
			var topic relay.Topic

			split := strings.Split(t, ":")
			if len(split) == 2 {
				topic.SourceTopic = split[0]
				topic.TargetTopic = split[1]
				topic.AutoTargetPartition = true
			} else if len(split) == 3 {
				topic.SourceTopic = split[0]
				topic.TargetTopic = split[1]
				topic.AutoTargetPartition = false

				p, err := strconv.Atoi(split[2])
				if err != nil {
					log.Fatalf("invalid topic:partition config in '%s'", t)
				}
				topic.TargetPartition = uint(p)
			} else {
				log.Fatalf("invalid topic '%s'. Should be in the format 'source:target' or 'source:target:partition'", t)
			}

			out[topic.SourceTopic] = topic
		}
	}

	if len(out) == 0 {
		log.Fatalf("no topic map specified")
	}

	return out
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
	if ko.String("mode") != "single" && len(ko.MapKeys("filters")) > 0 {
		log.Fatalf("filters can only be used in `single` mode.")
	}

	out := make(map[string]filter.Provider)
	for _, id := range ko.MapKeys("filters") {
		if !ko.Bool("filters." + id + ".enabled") {
			continue
		}

		path := ko.String("filters." + id + ".path")
		plg, err := plugin.Open(path)
		if err != nil {
			return nil, fmt.Errorf("error loading provider plugin '%s': %s: %v", id, path, err)
		}

		newFunc, err := plg.Lookup("New")
		if err != nil {
			return nil, fmt.Errorf("New() function not found in plugin '%s': %v", id, err)
		}
		f, ok := newFunc.(func([]byte) (interface{}, error))
		if !ok {
			return nil, fmt.Errorf("New() function is of invalid type (%T) in plugin '%s'", newFunc, id)
		}

		var cfg filter.Config
		if err := ko.Unmarshal("filters."+id, &cfg); err != nil {
			log.Fatalf("error unmarshalling filter config: %s: %v", id, err)
		}
		if cfg.Config == "" {
			lo.Info(fmt.Sprintf("WARNING: No config 'filters.%s' for '%s' in config", id, id))
		}

		// Initialize the plugin.
		prov, err := f([]byte(cfg.Config))
		if err != nil {
			return nil, fmt.Errorf("error initializing filter provider plugin '%s': %v", id, err)
		}
		lo.Info(fmt.Sprintf("loaded filter provider plugin '%s' from %s", id, id))

		p, ok := prov.(filter.Provider)
		if !ok {
			return nil, fmt.Errorf("New() function does not return a provider that satisfies filter.Provider (%T) in plugin '%s'", prov, id)
		}

		if p.ID() != id {
			return nil, fmt.Errorf("filter provider plugin ID doesn't match '%s' != %s", id, p.ID())
		}
		out[p.ID()] = p
	}

	return out, nil
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
