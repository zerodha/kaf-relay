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
	"github.com/twmb/franz-go/pkg/kgo"
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
		configPath            string
		mode                  string
		checkpoint, stopAtEnd bool
		filterPaths           []string
	)
	f.StringVar(&configPath, "config", "config.toml", "Path to the TOML configuration file")
	f.StringVar(&mode, "mode", "single", "single/failover")
	f.BoolVar(&checkpoint, "checkpoint", false, "Use checkpoint file or not")
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

	// create consumer manager
	m := &consumerManager{mode: mode, reconnectInProgress: StateDisconnected}

	// setup producer
	p, err := initProducer(ctx, cfg.Producer, logger)
	if err != nil {
		log.Fatalf("error starting producer: %v", err)
	}

	// setup consumer
	if err := initConsumer(ctx, m, cfg.Consumers, cfg.App.MaxFailovers, logger); err != nil {
		log.Fatalf("error starting consumer: %v", err)
	}

	relay := relay{
		consumerMgr:     m,
		producer:        p,
		producerBatchCh: make(chan *kgo.Record, cfg.Producer.BatchSize),
		producerBatch:   make([]*kgo.Record, 0, cfg.Producer.BatchSize),

		topics:  cfg.Topics,
		metrics: metrics.NewSet(),
		logger:  logger,

		maxRetries:     cfg.App.MaxFailovers,
		retryBackoffFn: retryBackoff(),

		lagMonitorFreq: cfg.App.LagMonitorFreq,
		lagThreshold:   cfg.App.LagThreshold,

		stopAtEnd:  stopAtEnd,
		endOffsets: make(map[string]map[int32]int64),
		filters:    filters,
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

	// cleanup
	for _, c := range m.c.clients {
		if c != nil {
			c.Close()
		}
	}
	p.client.Close()
}
