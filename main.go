package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	flag "github.com/spf13/pflag"
)

var (
	buildString = "unknown"
)

func main() {
	// Initialize config
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

	ko := koanf.New(".")
	if err := ko.Load(posflag.Provider(f, ".", ko), nil); err != nil {
		log.Fatalf("error reading flag config: %v", err)
	}

	// Version flag.
	if ko.Bool("version") {
		fmt.Println(buildString)
		os.Exit(0)
	}

	if ko.Bool("stop-at-end") && ko.String("mode") == ModeFailover {
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

	var cfg Config
	if err := ko.Unmarshal("", &cfg); err != nil {
		log.Fatalf("error marshalling application config: %v", err)
	}

	// If there are topics in the commandline flags, override the ones read from the file.
	if topics := ko.Strings("topic"); len(topics) > 0 {
		mp := map[string]string{}
		for _, t := range topics {
			split := strings.Split(t, ":")
			if len(split) != 2 {
				log.Fatalf("invalid topic '%s'. Should be in the format 'source:target'", t)
			}

			mp[split[0]] = split[1]
		}
		cfg.Topics = mp
	}

	// Initialized the structured logger.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     cfg.App.LogLevel,
	}))

	// Load the optional filter providers.
	filters, err := initFilterProviders(ko.Strings("filter"), ko, logger)
	if err != nil {
		log.Fatalf("error initializing filter provider: %v", err)
	}

	// Assign topic mapping
	var topics []string
	for t := range cfg.Topics {
		topics = append(topics, t)
	}

	// Set consumer topics
	for i := 0; i < len(cfg.Consumers); i++ {
		cfg.Consumers[i].Topics = topics
	}
	cfg.Producer.Topics = cfg.Topics

	// Create context with interrupts signals
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var (
		metr = metrics.NewSet()
	)

	// setup producer
	prod, err := initProducer(ctx, cfg.Producer, cfg.App.Backoff, metr, logger)
	if err != nil {
		log.Fatalf("error starting producer: %v", err)
	}
	prod.maxReqTime = cfg.App.MaxRequestDuration

	// setup offsets manager with the destination offsets
	destOffsets, err := prod.GetEndOffsets(ctx, cfg.App.MaxRequestDuration)
	if err != nil {
		log.Fatalf("error fetching destination offsets: %v", err)
	}
	offsetMgr := &offsetManager{Offsets: destOffsets.KOffsets()}

	// setup consumer hook, consumer
	hookCh := make(chan struct{}, 1)
	var n = make([]Node, len(cfg.Consumers))
	for i := 0; i < len(cfg.Consumers); i++ {
		n[i] = Node{
			Weight: -1,
			ID:     i,
		}
	}

	cons := &consumer{
		client:      nil, // init during track healthy
		cfgs:        cfg.Consumers,
		maxReqTime:  cfg.App.MaxRequestDuration,
		backoffCfg:  cfg.App.Backoff,
		offsetMgr:   offsetMgr,
		nodeTracker: NewNodeTracker(n),
		l:           logger,
	}

	// setup relay
	relay := relay{
		consumer: cons,
		producer: prod,

		unhealthyCh: hookCh,

		topics:  cfg.Topics,
		metrics: metr,
		logger:  logger,

		maxRetries: cfg.App.MaxFailovers,
		backoffCfg: cfg.App.Backoff,

		nodeHealthCheckFreq: cfg.App.NodeHealthCheckFreq,
		lagThreshold:        cfg.App.LagThreshold,
		maxReqTime:          cfg.App.MaxRequestDuration,

		stopAtEnd:   ko.Bool("stop-at-end"),
		srcOffsets:  make(map[string]map[int32]int64),
		destOffsets: destOffsets.KOffsets(),

		filters: filters,

		nodeCh: make(chan int, 1),
	}

	// Start metrics handler
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		relay.getMetrics(buf)

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		buf.WriteTo(w)
	})

	srv := http.Server{
		Addr:         cfg.App.MetricsServerAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("error starting server: %v", err)
		}
	}()

	// Start forwarder daemon
	if err := relay.Start(ctx); err != nil {
		relay.logger.Error("error starting relay", "err", err)
	}

	// shutdown server
	srv.Shutdown(ctx)

	// close underlying client connections
	relay.Close()

	logger.Info("done!")
}
