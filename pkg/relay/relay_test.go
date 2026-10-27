package relay_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/pkg/kafkatarget"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

var testLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// newCluster starts an in-memory Kafka cluster with a topic of the given partitions.
func newCluster(t *testing.T, topic string, partitions int32) *kfake.Cluster {
	t.Helper()

	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// produce writes one message per value to the given partition.
func produce(t *testing.T, c *kfake.Cluster, topic string, partition int32, values ...string) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, v := range values {
		rec := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(v)}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// newRelay wires a relay from the sources into the target, resuming from the target's
// high watermark like main.go does.
func newRelay(t *testing.T, sources []*kfake.Cluster, target relay.Target, tp relay.Topic) *relay.Relay {
	t.Helper()

	hw, err := target.GetHighWatermark(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var cfgs []relay.ConsumerCfg
	for _, s := range sources {
		cfgs = append(cfgs, relay.ConsumerCfg{KafkaCfg: relay.KafkaCfg{BootstrapBrokers: s.ListenAddrs(), SessionTimeout: time.Second}})
	}

	m := metrics.NewSet()
	pool, err := relay.NewSourcePool(relay.SourcePoolCfg{
		HealthCheckInterval: 100 * time.Millisecond,
		ReqTimeout:          time.Second,
		LagThreshold:        100,
		MaxRetries:          relay.IndefiniteRetry,
	}, cfgs, tp, hw[tp.TargetTopic], m, testLog)
	if err != nil {
		t.Fatal(err)
	}

	r, err := relay.NewRelay(relay.RelayCfg{}, pool, target, tp, nil, m, testLog)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// newKafkaTarget returns a Kafka target producing to c.
func newKafkaTarget(t *testing.T, c *kfake.Cluster, tp relay.Topic) *kafkatarget.Target {
	t.Helper()

	target, err := kafkatarget.New(context.Background(), relay.TargetCfg{ReqTimeout: time.Second}, relay.ProducerCfg{
		KafkaCfg:        relay.KafkaCfg{BootstrapBrokers: c.ListenAddrs(), SessionTimeout: 5 * time.Second},
		MaxRetries:      relay.IndefiniteRetry,
		FlushFrequency:  50 * time.Millisecond,
		MaxMessageBytes: 1 << 20,
		BatchSize:       100,
		BufferSize:      100,
		FlushBatchSize:  100,
	}, relay.Topics{tp.SourceTopic: tp}, metrics.NewSet(), testLog)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

// startRelay runs a relay until the test ends, or until the returned func is called.
func startRelay(t *testing.T, sources []*kfake.Cluster, target relay.Target, tp relay.Topic) func() {
	t.Helper()

	r := newRelay(t, sources, target, tp)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitFor polls until cond holds, then waits a little longer so duplicates can show up.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for messages")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
}

func values(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

// received consumes the topic on c and returns the values seen so far per partition.
func received(t *testing.T, c *kfake.Cluster, topic string) func() map[int32][]string {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu   sync.Mutex
		msgs = map[int32][]string{}
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		for {
			fetches := cl.PollFetches(context.Background())
			if fetches.IsClientClosed() {
				return
			}
			mu.Lock()
			fetches.EachRecord(func(r *kgo.Record) {
				msgs[r.Partition] = append(msgs[r.Partition], string(r.Value))
			})
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		cl.Close()
		<-done
	})

	return func() map[int32][]string {
		mu.Lock()
		defer mu.Unlock()

		out := make(map[int32][]string, len(msgs))
		for p, v := range msgs {
			out[p] = append([]string(nil), v...)
		}
		return out
	}
}

func count(m map[int32][]string) int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

func TestKafkaRelay(t *testing.T) {
	const topic = "orders"
	src1, src2 := newCluster(t, topic, 3), newCluster(t, topic, 3)
	dst := newCluster(t, topic, 3)

	tp := relay.Topic{SourceTopic: topic, TargetTopic: topic, AutoTargetPartition: true}
	target := newKafkaTarget(t, dst, tp)

	got := received(t, dst, topic)
	startRelay(t, []*kfake.Cluster{src1, src2}, target, tp)

	// Both sources carry the same stream, so each message is relayed once.
	want := map[int32][]string{0: values("a0", 2), 1: values("a1", 3), 2: values("a2", 1)}
	for p, v := range want {
		produce(t, src1, topic, p, v...)
		produce(t, src2, topic, p, v...)
	}
	waitFor(t, func() bool { return count(got()) >= 6 })

	// Take the first source down. The relay fails over to the second and continues
	// from where it left off.
	src1.Close()
	for p, v := range map[int32][]string{0: values("b0", 2), 2: values("b2", 2)} {
		produce(t, src2, topic, p, v...)
		want[p] = append(want[p], v...)
	}
	waitFor(t, func() bool { return count(got()) >= 10 })

	if g := got(); fmt.Sprint(g) != fmt.Sprint(want) {
		t.Fatalf("target partitions = %v, want %v", g, want)
	}
}

func TestKafkaRelayPartitionCountMismatch(t *testing.T) {
	const topic = "orders"
	src, dst := newCluster(t, topic, 3), newCluster(t, topic, 1)

	tp := relay.Topic{SourceTopic: topic, TargetTopic: topic, AutoTargetPartition: true}
	target := newKafkaTarget(t, dst, tp)
	produce(t, src, topic, 0, "a")

	r := newRelay(t, []*kfake.Cluster{src}, target, tp)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Start(ctx); !errors.Is(err, relay.ErrPartitionCountMismatch) {
		t.Fatalf("Start() = %v, want ErrPartitionCountMismatch", err)
	}
}
