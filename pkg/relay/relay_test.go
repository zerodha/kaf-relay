package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/zerodha/kaf-relay/pkg/kafkatarget"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

type testCluster struct {
	brokers *kfake.Cluster
	pClient *kgo.Client
	cClient *kgo.Client
	once    sync.Once
}

func newTestCluster() (*testCluster, error) {
	brokers, err := kfake.NewCluster(kfake.Ports(0))
	if err != nil {
		return nil, fmt.Errorf("start fake Kafka cluster: %w", err)
	}
	return &testCluster{brokers: brokers}, nil
}

func (c *testCluster) addresses() []string {
	return c.brokers.ListenAddrs()
}

func (c *testCluster) addProducer(addrs []string) error {
	client, err := kgo.NewClient(
		kgo.ProduceRequestTimeout(time.Second),
		kgo.RecordDeliveryTimeout(7*time.Second),
		kgo.SeedBrokers(addrs...),
	)
	if err != nil {
		return err
	}
	c.pClient = client
	return nil
}

func (c *testCluster) addConsumer(topic string, addrs []string) error {
	client, err := kgo.NewClient(
		kgo.ConsumeTopics(topic),
		kgo.SeedBrokers(addrs...),
		kgo.FetchMaxWait(50*time.Millisecond),
	)
	if err != nil {
		return err
	}
	c.cClient = client
	return nil
}

func (c *testCluster) close() {
	c.once.Do(func() {
		if c.cClient != nil {
			c.cClient.Close()
		}
		if c.pClient != nil {
			c.pClient.Close()
		}
		c.brokers.Close()
	})
}

func (c *testCluster) createTopic(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewCreateTopicsRequest()
	topicRequest := kmsg.NewCreateTopicsRequestTopic()
	topicRequest.ReplicationFactor = -1
	topicRequest.Topic = topic
	topicRequest.NumPartitions = 1
	req.Topics = append(req.Topics, topicRequest)

	_, err := req.RequestWith(ctx, c.pClient)
	return err
}

func (c *testCluster) produce(topic, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := c.pClient.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	})
	return results.FirstErr()
}

func waitForValue(t *testing.T, target *testCluster, expected string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		fetches := target.cClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			t.Fatal("target consumer client closed")
		}
		for _, fetchErr := range fetches.Errors() {
			t.Fatalf("target consumer error: %v", fetchErr.Err)
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			t.Logf("received target message: %s", record.Value)
			if bytes.Equal(record.Value, []byte(expected)) {
				return
			}
		}

		if err := ctx.Err(); err != nil {
			t.Fatalf("timed out waiting for target value %q: %v", expected, err)
		}
	}
}

func TestKafkaRelay(t *testing.T) {
	const (
		sourceTopic = "source-topic"
		targetTopic = "target-topic"
	)

	src1, err := newTestCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(src1.close)

	src2, err := newTestCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(src2.close)

	target, err := newTestCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(target.close)

	src1Addrs := src1.addresses()
	src2Addrs := src2.addresses()
	targetAddrs := target.addresses()
	for _, setup := range []struct {
		cluster *testCluster
		addrs   []string
	}{
		{src1, src1Addrs},
		{src2, src2Addrs},
		{target, targetAddrs},
	} {
		if err := setup.cluster.addProducer(setup.addrs); err != nil {
			t.Fatal(err)
		}
	}

	for _, cluster := range []*testCluster{src1, src2} {
		if err := cluster.createTopic(sourceTopic); err != nil {
			t.Fatal(err)
		}
	}
	if err := target.createTopic(targetTopic); err != nil {
		t.Fatal(err)
	}
	if err := target.addConsumer(targetTopic, targetAddrs); err != nil {
		t.Fatal(err)
	}

	// Seed both source clusters with the same first record so that the second
	// source can resume at offset 1 after the first source fails.
	for _, source := range []*testCluster{src1, src2} {
		if err := source.produce(sourceTopic, "key1", "value1"); err != nil {
			t.Fatal(err)
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metricsSet := metrics.NewSet()
	topics := relay.Topics{
		sourceTopic: {
			SourceTopic:         sourceTopic,
			TargetTopic:         targetTopic,
			AutoTargetPartition: true,
		},
	}
	producerCfg := relay.ProducerCfg{
		KafkaCfg: relay.KafkaCfg{
			BootstrapBrokers: targetAddrs,
			SessionTimeout:   6 * time.Second,
		},
		EnableIdempotency: true,
		CommitAck:         "cluster",
		FlushFrequency:    10 * time.Millisecond,
		MaxMessageBytes:   1_000_000,
		BatchSize:         10,
		BufferSize:        100,
		FlushBatchSize:    1,
	}
	targetCfg := relay.TargetCfg{
		ReqTimeout:    time.Second,
		EnableBackoff: true,
		BackoffMin:    10 * time.Millisecond,
		BackoffMax:    50 * time.Millisecond,
	}
	targetWriter, err := kafkatarget.New(context.Background(), targetCfg, producerCfg, topics, metricsSet, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := targetWriter.Close(); err != nil {
			t.Errorf("close target writer: %v", err)
		}
	})

	targetOffsets, err := targetWriter.GetHighWatermark(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sourcePool, err := relay.NewSourcePool(relay.SourcePoolCfg{
		HealthCheckInterval: 50 * time.Millisecond,
		ReqTimeout:          time.Second,
		LagThreshold:        1000,
		MaxRetries:          relay.IndefiniteRetry,
		EnableBackoff:       true,
		BackoffMin:          10 * time.Millisecond,
		BackoffMax:          50 * time.Millisecond,
	}, []relay.ConsumerCfg{
		{KafkaCfg: relay.KafkaCfg{BootstrapBrokers: src1Addrs, SessionTimeout: 6 * time.Second}},
		{KafkaCfg: relay.KafkaCfg{BootstrapBrokers: src2Addrs, SessionTimeout: 6 * time.Second}},
	}, topics[sourceTopic], targetOffsets[targetTopic], metricsSet, logger)
	if err != nil {
		t.Fatal(err)
	}
	relayRunner, err := relay.NewRelay(relay.RelayCfg{}, sourcePool, targetWriter, topics[sourceTopic], nil, metricsSet, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- relayRunner.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-relayDone:
		case <-time.After(10 * time.Second):
			t.Error("relay did not stop during cleanup")
		}
	})

	waitForValue(t, target, "value1", 10*time.Second)

	src1.close()
	if err := src2.produce(sourceTopic, "key2", "value2"); err != nil {
		t.Fatal(err)
	}
	waitForValue(t, target, "value2", 10*time.Second)
}
