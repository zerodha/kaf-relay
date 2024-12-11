package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
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

	// Create a global context with interrupts signals.
	globalCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize metrics set and start the HTTP server.
	metr := metrics.NewSet()
	metrSrv := initMetricsServer(metr, ko)
	go func() {
		if err := metrSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("error starting server: %v", err)
		}
	}()
	defer metrSrv.Shutdown(globalCtx)

	// Initialize the source and target Kafka config.
	consumerCfgs, prodConfig := initKafkaConfig(ko)

	// Initialize the source:target topic map from config.
	topics := initTopicsMap(ko)

	var wg sync.WaitGroup
	// Spawn a relay per topic
	for _, topic := range topics {
		topic := topic

		wg.Add(1)
		go func() {
			// Start the relay. This is an indefinitely blocking call.
			defer wg.Done()

			target, err := relay.NewTarget(globalCtx, initTargetConfig(ko), prodConfig, topics, metr, lo)
			if err != nil {
				log.Fatalf("error initializing target controller: %v", err)
			}

			hOf, err := target.GetHighWatermark()
			if err != nil {
				log.Fatalf("error getting destination high watermark: %v", err)
			}

			targetOffsets := hOf.KOffsets()
			hw, ok := targetOffsets[topic.TargetTopic]
			if !ok {
				log.Fatalf("error fetching end offset for target topic %s", topic.TargetTopic)
			}

			// Initialize the source Kafka (consumer) relay.
			srcPool, err := relay.NewSourcePool(initSourcePoolConfig(ko), consumerCfgs, topic, hw, metr, lo)
			if err != nil {
				log.Fatalf("error initializing source pool controller: %v", err)
			}

			// Initialize the Relay which orchestrates consumption from the sourcePool
			// and writing to the target pool.
			relay, err := relay.NewRelay(initRelayConfig(ko), srcPool, target, topic, filters, lo)
			if err != nil {
				log.Fatalf("error initializing relay controller: %v", err)
			}

			if err := relay.Start(globalCtx); err != nil {
				log.Fatalf("error starting relay controller: %v", err)
			}
		}()
	}

	wg.Wait()
	lo.Info("bye")
}
