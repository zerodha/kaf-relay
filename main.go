package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/VictoriaMetrics/metrics"
)

var (
	buildString = "unknown"
)

func main() {
	ko, cfg := initConfig()

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

	// Set source topics on consumers.
	var srcTopics []string
	for _, t := range cfg.Topics {
		srcTopics = append(srcTopics, t.SourceTopic)
	}
	for i := 0; i < len(cfg.Sources); i++ {
		cfg.Sources[i].Topics = srcTopics
	}

	// Set src:target topic map on the producer.
	// cfg.Target.targetTopics = cfg.Topics

	// Create context with interrupts signals.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize metrics.
	metr := metrics.NewSet()

	// Initialize the producer.
	prod, err := initProducer(ctx, cfg.Topics, cfg.Target, cfg.App.Backoff, metr, logger)
	if err != nil {
		log.Fatalf("error starting producer: %v", err)
	}
	prod.maxReqTime = cfg.App.MaxRequestDuration

	// Setup offsets manager with the destination offsets.
	destOffsets, err := prod.GetTargetOffsets(ctx, cfg.App.MaxRequestDuration)
	if err != nil {
		log.Fatalf("error fetching destination offsets: %v", err)
	}
	offsetMgr := &offsetManager{Offsets: destOffsets.KOffsets()}

	// Initialize the consumers.
	hookCh := make(chan struct{}, 1)
	var n = make([]Node, len(cfg.Sources))
	for i := 0; i < len(cfg.Sources); i++ {
		n[i] = Node{
			Weight: -1,
			ID:     i,
		}
	}

	// Initialize the Relay.
	relay := &Relay{
		consumer: &consumer{
			client:      nil, // init during track healthy
			cfgs:        cfg.Sources,
			maxReqTime:  cfg.App.MaxRequestDuration,
			backoffCfg:  cfg.App.Backoff,
			offsetMgr:   offsetMgr,
			nodeTracker: NewNodeTracker(n),
			log:         logger,
		},

		producer: prod,

		unhealthyCh: hookCh,

		topics:  cfg.TopicsMap,
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

	// Start metrics HTTP server.
	metrics := initMetricsServer(relay, cfg.App.MetricsServerAddr)

	// Start the blocking relay.
	if err := relay.Start(ctx); err != nil {
		log.Fatalf("error starting relay: %v", err)
	}

	metrics.Shutdown(ctx)
	relay.Close()

	logger.Info("done")
}
