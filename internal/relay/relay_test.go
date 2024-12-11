package relay

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Cluster struct {
	brokers *kfake.Cluster
	pClient *kgo.Client
	cClient *kgo.Client
}

func NewCluster(port int) (*Cluster, error) {
	// Setup kafka cluster
	c, err := kfake.NewCluster(
		kfake.Ports(port),
	)
	if err != nil {
		return nil, fmt.Errorf("error starting kfake cluster: %v", err)
	}
	return &Cluster{
		brokers: c,
	}, nil
}

func (c *Cluster) AddProducer(addrs []string) error {
	opts := []kgo.Opt{
		kgo.ProduceRequestTimeout(time.Second),
		kgo.RecordDeliveryTimeout(7 * time.Second), // break the :ProduceSync if it takes too long
		kgo.SeedBrokers(addrs...),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	c.pClient = cl
	return nil
}

func (c *Cluster) AddConsumer(topic string, addrs []string) error {
	opts := []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.SeedBrokers(addrs...),
		kgo.FetchMaxWait(time.Second),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	c.cClient = cl
	return nil
}

func (c *Cluster) Close() {
	if c.cClient != nil {
		c.cClient.Close()
	}
	if c.pClient != nil {
		c.pClient.Close()
	}

	c.brokers.Close()
}

func (c *Cluster) CreateTopic(topic string) error {
	req := kmsg.NewCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.ReplicationFactor = -1
	rt.Topic = topic
	rt.NumPartitions = 1
	req.Topics = append(req.Topics, rt)

	if _, err := req.RequestWith(context.TODO(), c.pClient); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) DeleteTopic(topic string) error {
	req := kmsg.NewDeleteTopicsRequest()
	rt := kmsg.NewDeleteTopicsRequestTopic()
	rt.Topic = &topic
	req.Topics = append(req.Topics, rt)

	if _, err := req.RequestWith(context.TODO(), c.pClient); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) Produce(topic, key, val string) error {
	res := c.pClient.ProduceSync(context.TODO(), &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(val),
	})

	return res.FirstErr()
}

// Helper function to verify messages on target
func verifyTargetMessage(t *testing.T, target *Cluster, expectedValue string, timeout time.Duration) {
	msgCh := make(chan []byte, 1)
	errCh := make(chan error, 1)

	go func() {
		fetches := target.cClient.PollFetches(context.TODO())
		if fetches.IsClientClosed() {
			errCh <- fmt.Errorf("client closed")
			return
		}

		for _, err := range fetches.Errors() {
			errCh <- err.Err
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			msgCh <- rec.Value
		}
	}()

	select {
	case msg := <-msgCh:
		t.Logf("received msg on target: %s", string(msg))
		if !bytes.Equal(msg, []byte(expectedValue)) {
			t.Fatalf("expected value %s, got %s", expectedValue, string(msg))
		}
	case err := <-errCh:
		t.Fatalf("error consuming from target: %v", err)
	case <-time.After(timeout):
		t.Fatal("timeout waiting for message")
	}
}

// Helper function to produce identical messages to both sources
func produceToAll(t *testing.T, topic, key, value string, sources ...*Cluster) {
	for _, src := range sources {
		if err := src.Produce(topic, key, value); err != nil {
			t.Logf("failed to produce to %v: %v", src, err)
		}
	}
}

// TestConfig holds test configuration for multiple clusters
type TestConfig struct {
	Cluster1Addrs []string
	Cluster2Addrs []string
	Cluster3Addrs []string
	SourceTopic   string
	TargetTopic   string
}

func TestKafkaRelay(t *testing.T) {
	cfg := TestConfig{
		Cluster1Addrs: []string{"localhost:9091"},
		Cluster2Addrs: []string{"localhost:9092"},
		Cluster3Addrs: []string{"localhost:9093"},
		SourceTopic:   "xyz",
		TargetTopic:   "xyz2",
	}

	src1, err := NewCluster(9091)
	if err != nil {
		t.Fatal(err)
	}
	src2, err := NewCluster(9092)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewCluster(9093)
	if err != nil {
		t.Fatal(err)
	}

	// Setup clusters
	for _, setup := range []struct {
		cluster *Cluster
		addrs   []string
	}{
		{src1, cfg.Cluster1Addrs},
		{src2, cfg.Cluster2Addrs},
		{target, cfg.Cluster3Addrs},
	} {
		if err := setup.cluster.AddProducer(setup.addrs); err != nil {
			t.Fatal(err)
		}
	}

	if err := target.AddConsumer(cfg.TargetTopic, cfg.Cluster3Addrs); err != nil {
		t.Fatal(err)
	}

	// Create topics
	for _, c := range []*Cluster{src1, src2} {
		if err := c.CreateTopic(cfg.SourceTopic); err != nil {
			t.Fatal(err)
		}
	}
	if err := target.CreateTopic(cfg.TargetTopic); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 3)

	// Test cases
	t.Run("normal-operation", func(t *testing.T) {
		// Write a msg on src 1, so that it is elected as a candidate first
		if err := src1.Produce(cfg.SourceTopic, "key1", "value1"); err != nil {
			t.Fatal(err)
		}
		verifyTargetMessage(t, target, "value1", 10*time.Second)
	})

	t.Run("failover-scenario", func(t *testing.T) {
		// Simulate cluster 1 failure by taking it down
		src1.Close()

		// Add two messages on cluster 2
		if err := src2.Produce(cfg.SourceTopic, "key1", "value1"); err != nil {
			t.Fatal(err)
		}
		if err := src2.Produce(cfg.SourceTopic, "key2", "value2"); err != nil {
			t.Fatal(err)
		}

		verifyTargetMessage(t, target, "value2", 10*time.Second)
	})
}

// func TestKafkaRelayRandomScenarios(t *testing.T) {
// 	cfg := TestConfig{
// 		Cluster1Addrs: []string{"localhost:9091"},
// 		Cluster2Addrs: []string{"localhost:9092"},
// 		Cluster3Addrs: []string{"localhost:9093"},
// 		SourceTopic:   "xyz",
// 		TargetTopic:   "xyz2",
// 	}

// 	src1, err := NewCluster(9091)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	src2, err := NewCluster(9092)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	target, err := NewCluster(9093)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// Setup clusters
// 	for _, setup := range []struct {
// 		cluster *Cluster
// 		addrs   []string
// 	}{
// 		{src1, cfg.Cluster1Addrs},
// 		{src2, cfg.Cluster2Addrs},
// 		{target, cfg.Cluster3Addrs},
// 	} {
// 		if err := setup.cluster.AddProducer(setup.addrs); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	if err := target.AddConsumer(cfg.TargetTopic, cfg.Cluster3Addrs); err != nil {
// 		t.Fatal(err)
// 	}

// 	// Create topics
// 	for _, c := range []*Cluster{src1, src2} {
// 		if err := c.CreateTopic(cfg.SourceTopic); err != nil {
// 			t.Fatal(err)
// 		}
// 	}
// 	if err := target.CreateTopic(cfg.TargetTopic); err != nil {
// 		t.Fatal(err)
// 	}

// 	time.Sleep(time.Second * 4)

// 	// t.Run("alternating-source-writes", func(t *testing.T) {
// 	// 	messages := []struct {
// 	// 		key, value string
// 	// 	}{
// 	// 		{"msg1", "value1"},
// 	// 		{"msg2", "value2"},
// 	// 		{"msg3", "value3"},
// 	// 	}

// 	// 	for i, msg := range messages {
// 	// 		// Alternate between sources for each message
// 	// 		if i%2 == 0 {
// 	// 			produceToAll(t, cfg.SourceTopic, msg.key, msg.value, src1)
// 	// 		} else {
// 	// 			produceToAll(t, cfg.SourceTopic, msg.key, msg.value, src2)
// 	// 		}

// 	// 		// Verify message appears exactly once on target
// 	// 		verifyTargetMessage(t, target, msg.value, 10*time.Second)
// 	// 	}
// 	// })

// 	t.Run("rapid-source-switching", func(t *testing.T) {
// 		// Start with both sources up
// 		src1.Close()
// 		src1, err = NewCluster(9091)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if err := src1.AddProducer(cfg.Cluster1Addrs); err != nil {
// 			t.Fatal(err)
// 		}

// 		src2.Close()
// 		src2, err = NewCluster(9092)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if err := src2.AddProducer(cfg.Cluster2Addrs); err != nil {
// 			t.Fatal(err)
// 		}

// 		// Write sequence of messages while rapidly switching sources
// 		sequence := []struct {
// 			key, value string
// 			action     func()
// 		}{
// 			{"k1", "v1", func() { src2.Close() }},               // Write to src1, take down src2
// 			{"k2", "v2", func() { src1.Close() }},               // Write to both, take down src1
// 			{"k3", "v3", func() { /* both down */ }},            // No writes possible
// 			{"k4", "v4", func() { src2, _ = NewCluster(9092) }}, // Bring up src2
// 			{"k5", "v5", func() { src1, _ = NewCluster(9091) }}, // Bring up src1
// 			{"k6", "v6", func() { src1.Close(); src2.Close() }}, // Take both down
// 			{"k7", "v7", func() { src1, _ = NewCluster(9091) }}, // Bring up src1
// 		}

// 		for _, step := range sequence {
// 			// Try to produce to both sources (some will fail depending on state)
// 			if src1 != nil {
// 				_ = src1.Produce(cfg.SourceTopic, step.key, step.value)
// 			}
// 			if src2 != nil {
// 				_ = src2.Produce(cfg.SourceTopic, step.key, step.value)
// 			}

// 			// Execute the state change action
// 			step.action()
// 			time.Sleep(time.Second) // Allow time for state change

// 			// Only verify messages that should have made it through
// 			if src1 != nil || src2 != nil {
// 				verifyTargetMessage(t, target, step.value, 10*time.Second)
// 			}
// 		}
// 	})

// 	// t.Run("concurrent-identical-messages", func(t *testing.T) {
// 	// 	// Ensure both sources are up
// 	// 	if src1 != nil {
// 	// 		src1.Close()
// 	// 	}
// 	// 	if src2 != nil {
// 	// 		src2.Close()
// 	// 	}

// 	// 	src1, _ = NewCluster(9091)
// 	// 	src2, _ = NewCluster(9092)
// 	// 	if err := src1.AddProducer(cfg.Cluster1Addrs); err != nil {
// 	// 		t.Fatal(err)
// 	// 	}
// 	// 	if err := src2.AddProducer(cfg.Cluster2Addrs); err != nil {
// 	// 		t.Fatal(err)
// 	// 	}

// 	// 	// Write identical messages to both sources concurrently
// 	// 	var wg sync.WaitGroup
// 	// 	messages := []struct {
// 	// 		key, value string
// 	// 	}{
// 	// 		{"concurrent1", "value1"},
// 	// 		{"concurrent2", "value2"},
// 	// 		{"concurrent3", "value3"},
// 	// 	}

// 	// 	for _, msg := range messages {
// 	// 		wg.Add(2)
// 	// 		go func(k, v string) {
// 	// 			defer wg.Done()
// 	// 			_ = src1.Produce(cfg.SourceTopic, k, v)
// 	// 		}(msg.key, msg.value)

// 	// 		go func(k, v string) {
// 	// 			defer wg.Done()
// 	// 			_ = src2.Produce(cfg.SourceTopic, k, v)
// 	// 		}(msg.key, msg.value)

// 	// 		wg.Wait()

// 	// 		// Verify message appears exactly once on target
// 	// 		verifyTargetMessage(t, target, msg.value, 10*time.Second)
// 	// 	}
// 	// })
// }
