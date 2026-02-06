package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type SourcePoolCfg struct {
	HealthCheckInterval time.Duration
	ReqTimeout          time.Duration
	LagThreshold        int64
	MaxRetries          int
	EnableBackoff       bool
	BackoffMin          time.Duration
	BackoffMax          time.Duration
}

// Server represents a source Server's config with health and weight
// parameters which are used for tracking health status.
type Server struct {
	Config ConsumerCfg
	ID     int

	// Weight is the cumulative high watermark (offset) of every single topic
	// on a source. This is used for comparing lags between different sources
	// based on a threshold. If a server is unhealthy, the weight is marked as -1.
	Weight int64

	Healthy bool

	// This is only set when a new live Kafka consumer connection is established
	// on demand via Get(), where a server{} is returned. Internally, no connections
	// are maintained on SourcePool.[]servers and only the config, weight etc.
	// params are used to keep track of healthy servers.
	Client *kgo.Client
}

// TopicOffsets defines partition->offset map for any single src/target kafka topic
type TopicOffsets map[int32]kgo.Offset

// SourcePool manages the source Kafka instances and consumption.
type SourcePool struct {
	cfg     SourcePoolCfg
	log     *slog.Logger
	metrics *metrics.Set

	// targetOffsets is initialized with current topic high watermarks from target.
	// These are updated whenever new msgs from src are sent to target for producing.
	// Whenever a new direct src consumer starts consuming from respective topic it uses
	// the offsets from this map. (These happen independently in the pool loop, hence no lock)
	topic         Topic
	targetOffsets TopicOffsets

	// List of all source servers.
	servers []Server

	// The server amongst all the given one's that is best placed to be the current
	// "healthiest". This is determined by weights (that track offset lag) and status
	// down. It's possible that curCandidate can itself be down or unhealthy,
	// for instance, when all sources are down. In such a scenario, the poll simply
	// keeps retriying until a real viable candidate is available.
	curCandidate Server
	lastSentID   int

	fetchCtx    context.Context
	cancelFetch context.CancelFunc

	backoffFn func(int) time.Duration
	sync.Mutex
}

const (
	unhealthyWeight int64 = -1
)

var (
	ErrorNoHealthy = errors.New("no healthy node")
)

// NewSourcePool returns a controller instance that manages the lifecycle of a pool of N source (consumer)
// servers. The pool always attempts to find one healthy node for the relay to consume from.
func NewSourcePool(cfg SourcePoolCfg, serverCfgs []ConsumerCfg, topic Topic, targetOffsets TopicOffsets, m *metrics.Set, log *slog.Logger) (*SourcePool, error) {
	servers := make([]Server, 0, len(serverCfgs))

	// Initially mark all servers as unhealthy.
	for n, c := range serverCfgs {
		servers = append(servers, Server{
			ID:      n,
			Weight:  unhealthyWeight,
			Healthy: false,
			Config:  c,
		})
	}

	sp := &SourcePool{
		cfg:       cfg,
		topic:     topic,
		servers:   servers,
		log:       log,
		metrics:   m,
		backoffFn: getBackoffFn(cfg.EnableBackoff, cfg.BackoffMin, cfg.BackoffMax),
	}

	sp.setInitialOffsets(targetOffsets)
	return sp, nil
}

// setInitialOffsets sets the offset/weight from the target on boot so that the messages
// can be consumed from the offsets where they were left off.
func (sp *SourcePool) setInitialOffsets(of TopicOffsets) {
	// Assign the current weight as initial target offset.
	// This is done to resume if target already has messages published from src.
	var w int64
	for _, o := range of {
		w += o.EpochOffset().Offset
	}

	// Set the current candidate with initial weight and a placeholder ID. This initial
	// weight ensures we resume consuming from where last left off. A real
	// healthy node should replace this via background checks
	sp.log.Debug("setting initial target node weight", "weight", w, "topics", of)
	sp.targetOffsets = of
	sp.curCandidate = Server{
		Healthy: false,
		Weight:  w,
	}
}

// Get attempts return a healthy source Kafka client connection.
// It internally applies backoff/retries between connection attempts and thus can take
// indefinitely long to return based on the config.
func (sp *SourcePool) Get(globalCtx context.Context) (*Server, error) {
	retries := 0
loop:
	for {
		select {
		case <-globalCtx.Done():
			return nil, globalCtx.Err()
		default:
			if sp.cfg.MaxRetries != IndefiniteRetry && retries >= sp.cfg.MaxRetries {
				return nil, fmt.Errorf("`max_retries`(%d) exhausted; exiting relay", sp.cfg.MaxRetries)
			}

			// Get the config for a healthy node.
			s, err := sp.getCurCandidate()
			if err == nil {
				sp.log.Debug("attempting new source connection", "id", s.ID, "broker", s.Config.BootstrapBrokers, "retries", retries)
				conn, err := sp.newConn(globalCtx, s)
				if err != nil {
					retries++
					sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcNetworkErrMetric, s.ID, "new connection failed")).Inc()
					sp.log.Error("new source connection failed", "id", s.ID, "broker", s.Config.BootstrapBrokers, "error", err, "retries", retries)
					waitTries(globalCtx, sp.backoffFn(retries))
					continue loop
				}

				out := s
				out.Client = conn

				// Lock because sp.cancelFetch could be accessed by healthcheck() goroutine.
				sp.Lock()
				sp.fetchCtx, sp.cancelFetch = context.WithCancel(globalCtx)
				sp.Unlock()
				return &out, nil
			}

			retries++
			sp.metrics.GetOrCreateCounter(SrcsUnhealthyMetric).Inc()
			sp.log.Error("no healthy server found. waiting and retrying", "retries", retries, "error", err)
			waitTries(globalCtx, sp.backoffFn(retries))
		}
	}
}

// GetFetches retrieves a Kafka fetch iterator to retrieve individual messages from.
func (sp *SourcePool) GetFetches(s *Server) (kgo.Fetches, error) {
	sp.log.Debug("retrieving fetches from source", "id", s.ID, "broker", s.Config.BootstrapBrokers)
	fetches := s.Client.PollFetches(sp.fetchCtx)

	// There's no connection.
	if fetches.IsClientClosed() {
		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcKafkaErrMetric, s.ID, "client closed")).Inc()
		sp.log.Debug("retrieving fetches failed. client closed.", "id", s.ID, "broker", s.Config.BootstrapBrokers)
		sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	// If there are errors in the fetches, handle them.
	for _, err := range fetches.Errors() {
		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcKafkaErrMetric, s.ID, "fetches error")).Inc()
		sp.log.Error("found error in fetches", "server", s.ID, "error", err.Err)
		sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	return fetches, nil
}

// RecordOffsets records the offsets of the latest fetched records per topic.
// This is used to resume consumption on new connections/reconnections from the source during runtime.
func (sp *SourcePool) RecordOffsets(rec *kgo.Record) error {
	if sp.targetOffsets == nil {
		sp.targetOffsets = make(TopicOffsets)
	}

	if sp.topic.SourceTopic != rec.Topic {
		return fmt.Errorf("target topic not found for src topic %s", rec.Topic)
	}
	sp.targetOffsets[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)

	return nil
}

func (sp *SourcePool) GetHighWatermark(ctx context.Context, cl *kgo.Client) (kadm.ListedOffsets, error) {
	return getHighWatermark(ctx, cl, []string{sp.topic.SourceTopic}, sp.cfg.ReqTimeout)
}

// newConn initializes a new consumer group config.
func (sp *SourcePool) newConn(ctx context.Context, s Server) (*kgo.Client, error) {
	sp.log.Debug("running TCP health check", "id", s.ID, "server", s.Config.BootstrapBrokers, "session_timeout", s.Config.SessionTimeout)
	if ok := checkTCP(ctx, s.Config.BootstrapBrokers, s.Config.SessionTimeout); !ok {
		return nil, ErrorNoHealthy
	}

	sp.log.Debug("initiazing new source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
	cl, err := sp.initConsumer(s.Config)
	if err != nil {
		sp.log.Error("error initiazing source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
		return nil, err
	}

	return cl, nil
}

// healthcheck indefinitely monitors the health of source (consumer) nodes and keeps the status
// and weightage of the nodes updated.
func (sp *SourcePool) healthcheck(ctx context.Context, signal chan struct{}) error {
	tick := time.NewTicker(sp.cfg.HealthCheckInterval)
	defer tick.Stop()

	// Copy the servers to be used in the infinite loop below so that the main list
	// of servers don't have to be locked.
	sp.Lock()
	servers := make([]Server, 0, len(sp.servers))
	servers = append(servers, sp.servers...)
	sp.Unlock()

	clients := make([]*kgo.Client, len(servers))
	for {
		select {
		case <-ctx.Done():
			sp.log.Debug("ending healthcheck goroutine")

			// Close all open admin Kafka clients.
			for _, cl := range clients {
				if cl != nil {
					cl.Close()
				}
			}

			return ctx.Err()

		case <-tick.C:
			// Fetch offset counts for each server.
			wg := &sync.WaitGroup{}

			currActiveWeight := unhealthyWeight
			for i, s := range servers {
				sp.log.Debug("running background health check", "id", s.ID, "server", s.Config.BootstrapBrokers)

				// For the first ever check, clients will be nil.
				if clients[i] == nil {
					sp.log.Debug("initializing admin client for background check", "id", s.ID, "server", s.Config.BootstrapBrokers)
					cl, err := sp.initConsumerClient(s.Config)
					if err != nil {
						sp.log.Error("error initializing admin client in background healthcheck", "id", s.ID, "server", s.Config.BootstrapBrokers, "error", err)
						continue
					}

					sp.log.Debug("initialized admin client for background check", "id", s.ID, "server", s.Config.BootstrapBrokers)
					clients[i] = cl
				}

				// Spawn a goroutine for the client to concurrently fetch its offsets. The waitgroup
				// ensures that offsets for all servers are fetched and then tallied together for healthcheck.
				wg.Add(1)
				go func(idx int, s Server) {
					defer wg.Done()

					// Get the highest offset of all the topics on the source server and sum them up
					// to derive the weight of the server.
					sp.log.Debug("getting high watermark via admin client for background check", "id", idx)
					offsets, err := sp.GetHighWatermark(ctx, clients[idx])
					if err != nil && offsets == nil {
						sp.log.Error("error fetching offset in background healthcheck", "id", s.ID, "server", s.Config.BootstrapBrokers, "error", err)
						sp.setWeight(servers[idx].ID, unhealthyWeight)
						// If the current candidate is no longer healthy,
						// signal relay to stop polling it.
						sp.Lock()
						var (
							shouldCancel = s.ID == sp.lastSentID
							cancelFn     = sp.cancelFetch
						)
						sp.Unlock()

						if shouldCancel && cancelFn != nil {
							cancelFn()
						}

						return
					}

					var weight int64 = 0
					offsets.Each(func(lo kadm.ListedOffset) {
						weight += lo.Offset
					})

					// NOTE: Check concurrency.
					servers[idx].Weight = weight

					// Adjust the global health of the servers.
					sp.setWeight(servers[idx].ID, weight)

					sp.Lock()
					if servers[idx].ID == sp.lastSentID {
						currActiveWeight = weight
					}
					sp.Unlock()
				}(i, s)
			}
			wg.Wait()

			// Now that offsets/weights for all servers are fetched, check if the current server
			// is lagging beyond the threshold.
			for _, s := range servers {
				// If the current server is now unhealthy skip checking for lag since we're in the
				// process of picking a new candidate.
				sp.Lock()
				lastSent := sp.lastSentID
				sp.Unlock()
				if lastSent == s.ID || currActiveWeight == unhealthyWeight {
					continue
				}

				sp.log.Debug("checking current server's lag", "id", s.ID, "server", s.Config.BootstrapBrokers, "s.weight", s.Weight, "curr", currActiveWeight, "diff", s.Weight-currActiveWeight, "threshold", sp.cfg.LagThreshold)
				if s.Weight-currActiveWeight > sp.cfg.LagThreshold {
					sp.log.Error("current server's lag threshold exceeded. Marking as unhealthy.", "id", s.ID, "server", s.Config.BootstrapBrokers, "diff", s.Weight-currActiveWeight > sp.cfg.LagThreshold, "threshold", sp.cfg.LagThreshold)
					sp.setWeight(s.ID, unhealthyWeight)

					// Cancel any active fetches.
					sp.Lock()
					cancelFn := sp.cancelFetch
					sp.Unlock()
					if cancelFn != nil {
						cancelFn()
					}

					// Signal the relay poll loop to start asking for a healthy client.
					// The push is non-blocking to avoid getting stuck trying to send on the poll loop
					// if the poll loop's subsection (checking for errors) has already sent a signal
					select {
					case signal <- struct{}{}:
					default:
					}
				}
			}
		}
	}
}

// initConsumer initializes a Kafka consumer client. This is used for creating consumer connection to source servers.
func (sp *SourcePool) initConsumer(cfg ConsumerCfg) (*kgo.Client, error) {
	// TODO: check if this grows
	cp := make(map[int32]kgo.Offset)
	for p, o := range sp.targetOffsets {
		cp[p] = kgo.NewOffset().At(o.EpochOffset().Offset)
	}

	sp.log.Info("initializing new source consumer", "offsets", cp, "brokers", cfg.BootstrapBrokers)
	opts := []kgo.Opt{
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{sp.topic.SourceTopic: cp}),
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(sp.cfg.ReqTimeout),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	if cfg.EnableAuth {
		opts = addSASLConfig(opts, cfg.KafkaCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := getTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
			if err != nil {
				return nil, err
			}

			// Set up TLS configuration.
			opts = append(opts, tlsOpt)
		}
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if err := testConnection(cl, cfg.SessionTimeout, []string{sp.topic.SourceTopic}, nil); err != nil {
		return nil, err
	}

	return cl, nil
}

// initConsumerClient returns franz-go client with default config.
func (sp *SourcePool) initConsumerClient(cfg ConsumerCfg) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(sp.cfg.ReqTimeout),
		kgo.SessionTimeout(cfg.SessionTimeout),
	}

	if cfg.EnableLog {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	}

	if cfg.EnableAuth {
		opts = addSASLConfig(opts, cfg.KafkaCfg)
	}

	if cfg.EnableTLS {
		if cfg.CACertPath == "" && cfg.ClientCertPath == "" && cfg.ClientKeyPath == "" {
			opts = append(opts, kgo.DialTLS())
		} else {
			tlsOpt, err := getTLSConfig(cfg.CACertPath, cfg.ClientCertPath, cfg.ClientKeyPath)
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

// getCurCandidate returns the most viable candidate server config (highest weight and not down).
// If everything is down, it returns the one with the highest weight.
func (sp *SourcePool) getCurCandidate() (Server, error) {
	sp.Lock()
	defer sp.Unlock()

	// If the weight (sum of all high watermarks of all topics on the source) is -1,
	// the server is unhealthy.
	if sp.curCandidate.Weight == unhealthyWeight || !sp.curCandidate.Healthy {
		return sp.curCandidate, ErrorNoHealthy
	}

	sp.lastSentID = sp.curCandidate.ID
	return sp.curCandidate, nil
}

// setWeight updates the weight (cumulative offset highwatermark for all topics on the server)
// for a particular server. If it's set to -1, the server is assumed to be unhealthy.
func (sp *SourcePool) setWeight(id int, weight int64) {
	sp.Lock()
	defer sp.Unlock()

	for _, s := range sp.servers {
		if s.ID != id {
			continue
		}

		s.Weight = weight
		if s.Weight != unhealthyWeight {
			s.Healthy = true
		}

		// If the incoming server's weight is greater than the current candidate,
		// promote that to the current candidate.
		if weight > sp.curCandidate.Weight {
			sp.curCandidate = s
		}

		sp.metrics.GetOrCreateCounter(fmt.Sprintf(SrcHealthMetric, id)).Set(uint64(weight))
		sp.log.Debug("setting candidate weight", "id", id, "weight", weight, "curr", sp.curCandidate)
		sp.servers[id] = s
		break
	}
}
