package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
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

	var configPath string
	f.StringVar(&configPath, "config", "config.toml", "Path to the TOML configuration file")
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

	// Unmarshall embedded structs
	if err := ko.UnmarshalWithConf("consumer", &cfg.Consumer.ClientConfig, koanf.UnmarshalConf{}); err != nil {
		log.Fatalf("error unmarshalling consumer config: %v", err)
	}
	if err := ko.UnmarshalWithConf("producer", &cfg.Producer.ClientConfig, koanf.UnmarshalConf{}); err != nil {
		log.Fatalf("error unmarshalling producer config: %v", err)
	}

	// Assign topic mapping
	var topics []string
	for t := range cfg.Topics {
		topics = append(topics, t)
	}

	cfg.Consumer.Topics = topics
	cfg.Producer.Topics = cfg.Topics

	c, err := initConsumer(cfg.Consumer)
	if err != nil {
		log.Fatalf("error starting consumer: %v", err)
	}

	p, err := initProducer(cfg.Producer)
	if err != nil {
		log.Fatalf("error starting producer: %v", err)
	}

	// Create context with interrupts signals
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	relay := relay{
		consumer: c,
		producer: p,
		topics:   cfg.Topics,
		metrics:  metrics.NewSet(),
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
		Addr:         ":7081",
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
	relay.Start(ctx)

	// shutdown server
	srv.Shutdown(ctx)

	// cleanup
	c.client.Close()
	p.client.Close()
}
