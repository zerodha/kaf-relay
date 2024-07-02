package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/VictoriaMetrics/metrics"
	"github.com/knadh/koanf/v2"
	"github.com/zerodha/kaf-relay/internal/relay"
)

var (
	buildString = "unknown"
	ko          = koanf.New(".")
)

func main() {
	// Initialize CLI flags.
	initFlags(ko)

	fmt.Println(buildString)
	if ko.Bool("version") {
		os.Exit(0)
	}

	// Read config files.
	initConfig(ko)

	// Initialized the structured lo.
	lo := initLog(ko)

	// Load the optional filter providers.
	filters, err := initFilters(ko, lo)
	if err != nil {
		log.Fatalf("error initializing filter provider: %v", err)
	}

	// Initialize metrics.
	metr := metrics.NewSet()

	// Create a global context with interrupts signals.
	globalCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize the source and target Kafka config.
	consumerCfgs, prodConfig := initKafkaConfig(ko)

	// Initialize the source:target topic map from config.
	topics := initTopicsMap(ko)

	// Initialize the target Kafka (producer) relay.
	target, err := relay.NewTarget(globalCtx, initTargetConfig(ko), prodConfig, topics, metr, lo)
	if err != nil {
		log.Fatalf("error initializing target controller: %v", err)
	}

	hOf, err := target.GetHighWatermark()
	if err != nil {
		log.Fatalf("error getting destination high watermark: %v", err)
	}

	// Initialize the source Kafka (consumer) relay.
	srcPool, err := relay.NewSourcePool(initSourcePoolConfig(ko), consumerCfgs, topics, lo)
	if err != nil {
		log.Fatalf("error initializing source pool controller: %v", err)
	}

	srcPool.SetInitialOffsets(hOf.KOffsets())

	// Initialize the Relay which orchestrates consumption from the sourcePool
	// and writing to the target pool.
	relay, err := relay.NewRelay(initRelayConfig(ko), srcPool, target, topics, filters, lo)
	if err != nil {
		log.Fatalf("error initializing relay controller: %v", err)
	}

	// Start the metrSrv HTTP server.
	metrSrv := initMetricsServer(metr, ko)

	// Start the relay. This is an indefinitely blocking call.
	if err := relay.Start(globalCtx); err != nil {
		log.Fatalf("error starting relay controller: %v", err)
	}

	if metrSrv != nil {
		metrSrv.Shutdown(globalCtx)
	}
	lo.Info("bye")
}
