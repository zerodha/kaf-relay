package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

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

	GroupID    string
	InstanceID string
}

// Server represents a source Server's config with health and weight
// parameters which are used for tracking health status.
type Server struct {
	Config ConsumerGroupCfg
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

// SourcePool manages the source Kafka instances and consumption.
type SourcePool struct {
	cfg    SourcePoolCfg
	client *kgo.Client
	log    *slog.Logger
	topics []string

	offsets map[string]map[int32]kgo.Offset

	// List of all source servers.
	servers []Server

	// The server amongst all the given one's that is best placed to be the current
	// "healthiest". This is determined by weights (that track offset lag) and status
	// down. It's possible that curCandidate can itself be down or unhealthy,
	// for instance, when all sources are down. In such a scenario, the poll simply
	// keeps retriying until a real viable candidate is available.
	curCandidate Server

	fetchCtx    context.Context
	fetchCancel context.CancelFunc

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
func NewSourcePool(cfg SourcePoolCfg, serverCfgs []ConsumerGroupCfg, topics Topics, log *slog.Logger) (*SourcePool, error) {
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

	topicNames := make([]string, 0, len(topics))
	for t := range topics {
		topicNames = append(topicNames, t)
	}

	return &SourcePool{
		cfg:       cfg,
		topics:    topicNames,
		servers:   servers,
		log:       log,
		backoffFn: getBackoffFn(cfg.EnableBackoff, cfg.BackoffMin, cfg.BackoffMax),
	}, nil
}

// SetInitialOffsets sets the offset/weight from the target on boot so that the messages
// can be consumed from the offsets where they were left off.
func (sp *SourcePool) SetInitialOffsets(of map[string]map[int32]kgo.Offset) {
	// Assign the current weight as initial target offset.
	// This is done to resume if target already has messages published from src.
	var w int64
	for _, p := range of {
		for _, o := range p {
			w += o.EpochOffset().Offset
		}
	}

	sp.offsets = of

	// Set the current candidate with initial weight and a placeholder ID. This initial
	// weight ensures we resume consuming from where last left off. A real
	// healthy node should replace this via background checks
	sp.log.Debug("setting initial target node weight", "weight", w)
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
	for {
		select {
		case <-globalCtx.Done():
			return nil, globalCtx.Err()
		default:
			if sp.cfg.MaxRetries != IndefiniteRetry && retries >= sp.cfg.MaxRetries {
				return nil, fmt.Errorf("`max_retries`(%d) exhausted; exiting relay", sp.cfg.MaxRetries)
			}

			// Get the config for a healthy node.
			if s, err := sp.getCurCandidate(); err == nil {
				sp.log.Debug("attempting new source connection", "id", s.ID, "broker", s.Config.BootstrapBrokers, "retries", retries)
				conn, err := sp.newConn(globalCtx, s)
				if err != nil {
					retries++
					sp.log.Error("new source connection failed", "id", s.ID, "broker", s.Config.BootstrapBrokers, "error", err, "retries", retries)
					waitTries(globalCtx, sp.backoffFn(retries))
				}

				// Cache the current live connection internally.
				sp.client = conn

				out := s
				out.Client = conn

				sp.fetchCtx, sp.fetchCancel = context.WithCancel(globalCtx)
				return &out, nil
			}

			retries++
			sp.log.Error("no healthy server found. waiting and retrying", "retries", retries)
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
		sp.log.Debug("retrieving fetches failed. client closed.", "id", s.ID, "broker", s.Config.BootstrapBrokers)
		sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	// If there are errors in the fetches, handle them.
	for _, err := range fetches.Errors() {
		sp.log.Error("found error in fetches", "server", s.ID, "error", err.Err)
		sp.setWeight(s.ID, unhealthyWeight)

		return nil, errors.New("fetch failed")
	}

	return fetches, nil
}

// RecordOffsets records the offsets of the latest fetched records per topic.
// This is used to resume consumption on new connections/reconnections from the source during runtime.
func (sp *SourcePool) RecordOffsets(rec *kgo.Record) {
	oMap := make(map[int32]kgo.Offset)
	oMap[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
	if sp.offsets != nil {
		if o, ok := sp.offsets[rec.Topic]; ok {
			o[rec.Partition] = oMap[rec.Partition]
			sp.offsets[rec.Topic] = o
		} else {
			sp.offsets[rec.Topic] = oMap
		}
	} else {
		sp.offsets = make(map[string]map[int32]kgo.Offset)
		sp.offsets[rec.Topic] = oMap
	}
}

func (sp *SourcePool) GetHighWatermark(ctx context.Context, cl *kgo.Client) (kadm.ListedOffsets, error) {
	return getHighWatermark(ctx, cl, sp.topics, sp.cfg.ReqTimeout)
}

// Close closes the active source Kafka client.
func (sp *SourcePool) Close() {
	if sp.client != nil {
		// Prevent blocking on close.
		sp.client.PurgeTopicsFromConsuming()
	}
}

// newConn initializes a new consumer group config.
func (sp *SourcePool) newConn(ctx context.Context, s Server) (*kgo.Client, error) {
	sp.log.Debug("running TCP health check", "id", s.ID, "server", s.Config.BootstrapBrokers, "session_timeout", s.Config.SessionTimeout)
	if ok := checkTCP(ctx, s.Config.BootstrapBrokers, s.Config.SessionTimeout); !ok {
		return nil, ErrorNoHealthy
	}

	sp.log.Debug("initiazing new source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
	cl, err := sp.initConsumerGroup(ctx, s.Config)
	if err != nil {
		sp.log.Error("error initiazing source consumer", "id", s.ID, "server", s.Config.BootstrapBrokers)
		return nil, err
	}

	if sp.offsets != nil {
		sp.log.Debug("resetting cached offsets", "id", s.ID, "server", s.Config.BootstrapBrokers, "offsets", sp.offsets)
		if err := sp.leaveAndResetOffsets(ctx, cl, s); err != nil {
			sp.log.Error("error resetting cached offsets", "id", s.ID, "server", s.Config.BootstrapBrokers, "error", err)
			return nil, err
		}

		sp.log.Debug("initiazing new source consumer after clearing offsets", "id", s.ID, "server", s.Config.BootstrapBrokers)
		cl, err = sp.initConsumerGroup(ctx, s.Config)
		if err != nil {
			sp.log.Error("error initiazing source consumer after offset reset", "id", s.ID, "server", s.Config.BootstrapBrokers)
			return nil, err
		}
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

			curServerWeight := unhealthyWeight
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

					if servers[idx].ID == sp.curCandidate.ID {
						curServerWeight = weight
					}
				}(i, s)
			}
			wg.Wait()

			// Now that offsets/weights for all servers are fetched, check if the current server
			// is lagging beyond the threshold.
			for _, s := range servers {
				if sp.curCandidate.ID == s.ID {
					continue
				}

				if s.Weight-curServerWeight > sp.cfg.LagThreshold {
					sp.log.Error("current server's lag threshold exceeded. Marking as unhealthy.", "id", s.ID, "server", s.Config.BootstrapBrokers, "diff", s.Weight-curServerWeight > sp.cfg.LagThreshold, "threshold", sp.cfg.LagThreshold)
					sp.setWeight(s.ID, unhealthyWeight)

					// Cancel any active fetches.
					sp.fetchCancel()

					// Signal the relay poll loop to start asking for a healthy client.
					signal <- struct{}{}
				}
			}
		}
	}
}

// initConsumerGroup initializes a Kafka consumer group. This is used for creating consumer connection to source servers.
func (sp *SourcePool) initConsumerGroup(ctx context.Context, cfg ConsumerGroupCfg) (*kgo.Client, error) {
	assingedCtx, cancelFn := context.WithTimeout(ctx, cfg.SessionTimeout)
	defer cancelFn()

	onAssigned := func(childCtx context.Context, cl *kgo.Client, claims map[string][]int32) {
		select {
		case <-ctx.Done():
			return
		case <-childCtx.Done():
			return
		default:
			sp.log.Debug("partition assigned", "broker", cl.OptValue(kgo.SeedBrokers), "claims", claims)
			cancelFn()
		}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapBrokers...),
		kgo.FetchMaxWait(sp.cfg.ReqTimeout),
		kgo.ConsumeTopics(sp.topics...),
		kgo.ConsumerGroup(sp.cfg.GroupID),
		kgo.InstanceID(sp.cfg.InstanceID),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(onAssigned),
		kgo.BlockRebalanceOnPoll(),
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

	if err := testConnection(cl, cfg.SessionTimeout, sp.topics, nil); err != nil {
		return nil, err
	}

	sp.log.Debug("waiting for source partition assignment", "server", cfg.BootstrapBrokers)
	<-assingedCtx.Done()
	if assingedCtx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("timeout waiting for partition assingnment in target: %v: %w", cfg.BootstrapBrokers, assingedCtx.Err())
	}
	sp.log.Debug("partition assigned", "server", cfg.BootstrapBrokers)

	return cl, nil
}

// initConsumerClient returns franz-go client with default config.
func (sp *SourcePool) initConsumerClient(cfg ConsumerGroupCfg) (*kgo.Client, error) {
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
		if weight >= sp.curCandidate.Weight {
			sp.curCandidate = s
		}

		sp.log.Debug("setting candidate weight", "id", id, "weight", weight, "curr", sp.curCandidate)
		sp.servers[id] = s
		break
	}
}

// leaveAndResetOffsets leaves the current consumer group and resets its offset if given.
func (sp *SourcePool) leaveAndResetOffsets(ctx context.Context, cl *kgo.Client, s Server) error {
	// leave group; mark the group as `Empty` before attempting to reset offsets.
	sp.log.Debug("leaving group", "id", s.ID, "server", s.Config.BootstrapBrokers)
	if err := sp.leaveGroup(ctx, cl); err != nil {
		return err
	}

	// Reset consumer group offsets using the existing offsets
	if sp.offsets != nil {
		sp.log.Debug("resetting offsets", "id", s.ID, "server", s.Config.BootstrapBrokers, "offsets", sp.offsets)
		if err := sp.resetOffsets(ctx, cl, s); err != nil {
			return err
		}
	}

	return nil
}

// resetOffsets resets the consumer group with the given offsets map.
// Also waits for topics to catch up to the messages in case it is lagging behind.
func (sp *SourcePool) resetOffsets(ctx context.Context, cl *kgo.Client, s Server) error {
	var (
		maxAttempts = -1 // TODO: make this configurable?
		attempts    = 0
		admCl       = kadm.NewClient(cl)
	)

	// wait for topic lap to catch up
waitForTopicLag:
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if attempts >= maxAttempts && maxAttempts != IndefiniteRetry {
				return fmt.Errorf("max attempts(%d) for fetching offsets", maxAttempts)
			}

			// Get end offsets of the topics
			topicOffsets, err := admCl.ListEndOffsets(ctx, sp.topics...)
			if err != nil {
				sp.log.Error("error fetching offsets", "err", err)
				return err
			}

			for t, po := range sp.offsets {
				for p, o := range po {
					eO, ok := topicOffsets.Lookup(t, p)
					// TODO:
					if !ok {
						continue
					}

					if o.EpochOffset().Offset > eO.Offset {
						return fmt.Errorf("%w by %d msgs(s)", ErrLaggingBehind, o.EpochOffset().Offset-eO.Offset)
					}
				}
			}

			break waitForTopicLag
		}
	}

	// force set consumer group offsets
	of := make(kadm.Offsets)
	for t, po := range sp.offsets {
		oMap := make(map[int32]kgo.EpochOffset)
		for p, o := range po {
			oMap[p] = o.EpochOffset()
			of.AddOffset(t, p, o.EpochOffset().Offset, -1)
		}
	}

	sp.log.Info("resetting offsets for consumer group", "id", s.ID, "server", s.Config.BootstrapBrokers, "offsets", of)
	resp, err := admCl.CommitOffsets(ctx, sp.cfg.GroupID, of)
	if err != nil {
		sp.log.Error("error resetting group offset", "err", err)
	}

	if err := resp.Error(); err != nil {
		sp.log.Error("error resetting group offset", "err", err)
	}

	// _ = resp
	// // check for errors in offset responses
	// for _, or := range resp {
	// 	for _, r := range or {
	// 		if r.Err != nil {
	// 			l.Error("error resetting group offset", "err", r.Err)
	// 			return err
	// 		}
	// 	}
	// }

	return nil
}

// leaveGroup makes the given client leave from a consumer group.
func (sp *SourcePool) leaveGroup(ctx context.Context, cl *kgo.Client) error {
	l := kadm.LeaveGroup(sp.cfg.GroupID).Reason("resetting offsets").InstanceIDs(sp.cfg.InstanceID)

	resp, err := kadm.NewClient(cl).LeaveGroup(ctx, l)
	if err != nil {
		return err
	}

	if err := resp.Error(); err != nil {
		return err
	}

	return nil
}
