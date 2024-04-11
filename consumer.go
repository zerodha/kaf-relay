package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	// .GetHealthy returns this error when there is no healthy node
	ErrorNoHealthy = errors.New("no healthy node")
)

// consumer represents the kafka consumer client.
type consumer struct {
	client     *kgo.Client
	cfgs       []ConsumerGroupCfg
	backoffCfg BackoffCfg
	maxReqTime time.Duration

	offsetMgr   *offsetManager
	nodeTracker *NodeTracker

	log *slog.Logger
}

// offsetManager is a holder for the topic offsets.
type offsetManager struct {
	Offsets map[string]map[int32]kgo.Offset
}

// Get returns the topic offsets.
func (c *consumer) GetOffsets() map[string]map[int32]kgo.Offset {
	return c.offsetMgr.Offsets
}

// RecordOffsets tracks the offset for this record inmemory.
func (c *consumer) RecordOffsets(rec *kgo.Record) {
	oMap := make(map[int32]kgo.Offset)
	oMap[rec.Partition] = kgo.NewOffset().At(rec.Offset + 1)
	if c.offsetMgr.Offsets != nil {
		if o, ok := c.offsetMgr.Offsets[rec.Topic]; ok {
			o[rec.Partition] = oMap[rec.Partition]
			c.offsetMgr.Offsets[rec.Topic] = o
		} else {
			c.offsetMgr.Offsets[rec.Topic] = oMap
		}
	} else {
		c.offsetMgr.Offsets = make(map[string]map[int32]kgo.Offset)
		c.offsetMgr.Offsets[rec.Topic] = oMap
	}
}

// Close closes the kafka client.
func (c *consumer) Close() {
	if c.client != nil {
		// prevent blocking on close
		c.client.PurgeTopicsFromConsuming()
	}
}

func (c *consumer) GetHealthy(ctx context.Context) (int, error) {
	n := c.nodeTracker.GetHealthy()
	if n.Weight == -1 || n.Down {
		return -1, ErrorNoHealthy
	}

	return n.ID, nil
}

// reinit reinitializes the consumer group
func (c *consumer) Connect(ctx context.Context, cfg ConsumerGroupCfg) error {
	c.log.Debug("reinitializing consumer group", "broker", cfg.BootstrapBrokers)

	// tcp health check
	if ok := healthcheck(ctx, cfg.BootstrapBrokers, c.maxReqTime); !ok {
		return ErrorNoHealthy
	}

	cl, err := initConsumerGroup(ctx, cfg, c.log)
	if err != nil {
		return err
	}

	offsets := c.GetOffsets()
	if offsets != nil {
		err = leaveAndResetOffsets(ctx, cl, cfg, offsets, c.log)
		if err != nil {
			c.log.Error("error resetting offsets", "err", err)
			return err
		}

		cl, err = initConsumerGroup(ctx, cfg, c.log)
		if err != nil {
			return err
		}
	}

	c.client = cl

	return nil
}

// Node represents a node with its weight
type Node struct {
	Down   bool
	ID     int
	Weight int
}

// NodeTracker keeps track of the healthiest node
type NodeTracker struct {
	sync.Mutex

	nodes      map[int]Node
	healthiest Node
}

// NewNodeTracker creates a new NodeTracker
func NewNodeTracker(nodes []Node) *NodeTracker {
	n := make(map[int]Node)
	for _, node := range nodes {
		n[node.ID] = node
	}

	return &NodeTracker{
		nodes:      n,
		healthiest: Node{Down: true, Weight: -1},
	}
}

// GetHealthy returns the healthiest node without blocking
func (h *NodeTracker) GetHealthy() Node {
	h.Lock()
	node := h.healthiest
	h.Unlock()

	return node
}

func (h *NodeTracker) PushUp(nodeID int, weight int) bool {
	return h.updateWeight(nodeID, weight, false)
}

func (h *NodeTracker) PushDown(nodeID int) bool {
	return h.updateWeight(nodeID, -1, true)
}

// updateWeight updates the weight for a particular node
func (h *NodeTracker) updateWeight(nodeID int, weight int, down bool) bool {
	h.Lock()
	defer h.Unlock()

	node, ok := h.nodes[nodeID]
	if !ok {
		return false
	}

	node.Down = down
	node.Weight = weight
	h.nodes[nodeID] = node

	if h.healthiest.ID == nodeID {
		h.healthiest = node
		return false
	}

	// assign if the current node is the healthiest
	if (weight >= h.healthiest.Weight || h.healthiest.Down) && !down {
		h.healthiest = node
	}

	return true
}
