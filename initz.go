package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	flag "github.com/spf13/pflag"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/filter"
)

func initConfig() (*koanf.Koanf, Config) {
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

	return ko, cfg
}

// initProducer initializes the kafka producer client.
func initProducer(ctx context.Context, pCfg ProducerCfg, bCfg BackoffCfg, m *metrics.Set, l *slog.Logger) (*producer, error) {
	l.Info("creating producer", "broker", pCfg.BootstrapBrokers)

	p := &producer{
		cfg:        pCfg,
		backoffCfg: bCfg,
		logger:     l,
		metrics:    m,
		batch:      make([]*kgo.Record, 0, pCfg.BatchSize),
		//batchCh:    make(chan *kgo.Record, pCfg.BatchSize),
		batchCh: make(chan *kgo.Record),
	}
	cl, err := getProducerClient(ctx, pCfg, bCfg, l)
	if err != nil {
		return nil, err
	}

	p.client = cl

	return p, nil
}

// initConsumerGroup initializes a Kafka consumer group.
func initConsumerGroup(ctx context.Context, cfg ConsumerGroupCfg, l *slog.Logger) (*kgo.Client, error) {
	assingedCtx, cancelFn := context.WithTimeout(ctx, cfg.SessionTimeout)
	defer cancelFn()

	onAssigned := func(childCtx context.Context, cl *kgo.Client, claims map[string][]int32) {
		select {
		case <-ctx.Done():
			return
		case <-childCtx.Done():
			return
		default:
			l.Debug("partition assigned", "broker", cl.OptValue(kgo.SeedBrokers), "claims", claims)
			cancelFn()
		}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.InstanceID(cfg.InstanceID),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(onAssigned),
		kgo.BlockRebalanceOnPoll(),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := createTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if err := testConnection(cl, cfg.SessionTimeout, cfg.Topics, nil, false); err != nil {
		return nil, err
	}

	l.Debug("waiting till partition assigned", "broker", cl.OptValue(kgo.SeedBrokers))
	<-assingedCtx.Done()
	if assingedCtx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("timeout waiting for partition assingnment; broker %v: %w", cfg.BootstrapBrokers, assingedCtx.Err())
	}
	l.Debug("partition assingment done", "broker", cl.OptValue(kgo.SeedBrokers))

	return cl, nil
}

// initFilterProviders loads the go plugin, initializes it and return a map of filter plugins.
func initFilterProviders(names []string, ko *koanf.Koanf, log *slog.Logger) (map[string]filter.Provider, error) {
	out := make(map[string]filter.Provider)
	for _, fName := range names {
		plg, err := plugin.Open(fName)
		if err != nil {
			return nil, fmt.Errorf("error loading provider plugin '%s': %v", fName, err)
		}
		id := strings.TrimSuffix(filepath.Base(fName), filepath.Ext(fName))

		newFunc, err := plg.Lookup("New")
		if err != nil {
			return nil, fmt.Errorf("New() function not found in plugin '%s': %v", id, err)
		}
		f, ok := newFunc.(func([]byte) (interface{}, error))
		if !ok {
			return nil, fmt.Errorf("New() function is of invalid type (%T) in plugin '%s'", newFunc, id)
		}

		var cfg filter.Config
		ko.Unmarshal("filter."+id, &cfg)
		if cfg.Config == "" {
			log.Info(fmt.Sprintf("WARNING: No config 'filter.%s' for '%s' in config", id, id))
		}

		// Initialize the plugin.
		prov, err := f([]byte(cfg.Config))
		if err != nil {
			return nil, fmt.Errorf("error initializing filter provider plugin '%s': %v", id, err)
		}
		log.Info(fmt.Sprintf("loaded filter provider plugin '%s' from %s", id, fName))

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

func initMetricsServer(relay *Relay, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		relay.getMetrics(buf)

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		buf.WriteTo(w)
	})

	srv := &http.Server{
		Addr:         addr,
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

	return srv
}

// getProducerClient returns a Kafka producer client.
func getProducerClient(ctx context.Context, cfg ProducerCfg, bCfg BackoffCfg, l *slog.Logger) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.ProduceRequestTimeout(cfg.SessionTimeout),
		kgo.RecordDeliveryTimeout(cfg.SessionTimeout), // break the :ProduceSync if it takes too long
		kgo.ProducerBatchMaxBytes(int32(cfg.MaxMessageBytes)),
		kgo.MaxBufferedRecords(cfg.FlushBatchSize),
		kgo.ProducerLinger(cfg.FlushFrequency),
		kgo.ProducerBatchCompression(getCompressionCodec(cfg.Compression)),
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.RequiredAcks(getAckPolicy(cfg.CommitAck)),
	}

	// TCPAck/LeaderAck requires Kafka deduplication to be turned off.
	if !cfg.EnableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := createTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	var (
		retries = 0
		backoff = getBackoffFn(bCfg)
		err     error
		cl      *kgo.Client
	)

	// Retry until a successful connection.
outerLoop:
	for retries < cfg.MaxRetries || cfg.MaxRetries == IndefiniteRetry {
		select {
		case <-ctx.Done():
			break outerLoop
		default:
			cl, err = kgo.NewClient(opts...)
			if err != nil {
				l.Error("error creating producer client", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			// Get the destination topics
			var topics []string
			for _, v := range cfg.Topics {
				topics = append(topics, v)
			}

			// Test connectivity and ensure destination topics exists.
			err = testConnection(cl, cfg.SessionTimeout, topics, cfg.TopicsPartition, true)
			if err != nil {
				l.Error("error connecting to producer", "err", err)
				retries++
				waitTries(ctx, backoff(retries))
				continue
			}

			if err == nil {
				break outerLoop
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return cl, nil
}

// getClient returns franz-go client with default config.
func getClient(cfg ConsumerGroupCfg) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(cfg.MaxWaitTime),
		kgo.SessionTimeout(cfg.SessionTimeout),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	if cfg.EnableAuth {
		opts = appendSASL(opts, cfg.ClientCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := createTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration
			opts = append(opts, tlsOpt)
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return client, err
}
