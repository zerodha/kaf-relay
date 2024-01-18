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
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
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

	var (
		configPath  string
		mode        string
		stopAtEnd   bool
		filterPaths []string
	)
	f.StringVar(&configPath, "config", "config.toml", "Path to the TOML configuration file")
	f.StringVar(&mode, "mode", "single", "single/failover")
	f.BoolVar(&stopAtEnd, "stop-at-end", false, "Stop relay at the end of offsets")
	f.StringSliceVar(&filterPaths, "filter", []string{}, "Path to filter providers. Can specify multiple values.")
	f.Bool("version", false, "Current version of the build")

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Fatalf("error loading flags: %v", err)
	}

	// Version flag.
	if ok, _ := f.GetBool("version"); ok {
		fmt.Println(buildString)
		os.Exit(0)
	}

	if stopAtEnd && mode == ModeFailover {
		log.Fatalf("`--stop-at-end` cannot be used with `failover` mode")
	}

	// Load the config file.
	ko := koanf.New(".")
	log.Printf("reading config: %s", configPath)
	if err := ko.Load(file.Provider(configPath), toml.Parser()); err != nil {
		log.Fatalf("error reading config: %v", err)
	}

	var cfg Config
	if err := ko.Unmarshal("", &cfg); err != nil {
		log.Fatalf("error marshalling application config: %v", err)
	}

	// setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     cfg.App.LogLevel,
	}))

	// setup filter providers
	filters, err := initFilterProviders(filterPaths, ko, logger)
	if err != nil {
		log.Fatalf("error initializing filter provider: %v", err)
	}

	// Assign topic mapping
	var consumerTopics []string
	for c := range cfg.Topics {
		consumerTopics = append(consumerTopics, c)
	}

	// Set consumer topics
	for i := 0; i < len(cfg.Consumers); i++ {
		cfg.Consumers[i].Topics = consumerTopics
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
	destOffsets, err := prod.getOffsetsForConsumer(ctx, cfg.App.MaxRequestDuration)
	if err != nil {
		log.Fatalf("error fetching destination offsets: %v", err)
	}

	// setup consumer hook, consumer
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
		offsets:     destOffsets.KOffsets(),
		nodeTracker: NewNodeTracker(n),
		l:           logger,
	}

	// setup relay
	relay := relay{
		consumer: cons,
		producer: prod,

		unhealthyCh: make(chan struct{}, 1),

		topics:  cfg.Topics,
		metrics: metr,
		logger:  logger,

		maxRetries: cfg.App.MaxFailovers,
		backoffCfg: cfg.App.Backoff,

		nodeHealthCheckFreq: cfg.App.NodeHealthCheckFreq,
		lagThreshold:        cfg.App.LagThreshold,
		maxReqTime:          cfg.App.MaxRequestDuration,

		stopAtEnd:   stopAtEnd,
		srcOffsets:  make(map[string]map[int32]int64),
		destOffsets: destOffsets.KOffsets(),

		filters: filters,
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
