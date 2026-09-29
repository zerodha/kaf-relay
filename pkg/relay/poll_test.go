package relay

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// clientGoroutines counts goroutines owned by franz-go clients, ignoring the fake broker's own.
func clientGoroutines() int {
	buf := make([]byte, 1<<20)
	n := 0
	for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
		if strings.Contains(g, "franz-go/pkg/kgo.") {
			n++
		}
	}
	return n
}

func TestReconnectClosesSourceClients(t *testing.T) {
	const topic = "orders"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: []byte("a")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	baseline := clientGoroutines()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.NewSet()
	tp := Topic{SourceTopic: topic, TargetTopic: topic, AutoTargetPartition: true}
	pool, err := NewSourcePool(SourcePoolCfg{
		HealthCheckInterval: 50 * time.Millisecond,
		ReqTimeout:          time.Second,
		LagThreshold:        100,
		MaxRetries:          IndefiniteRetry,
	}, []ConsumerCfg{{KafkaCfg: KafkaCfg{BootstrapBrokers: c.ListenAddrs(), SessionTimeout: time.Second}}}, tp, nil, m, log)
	if err != nil {
		t.Fatal(err)
	}
	target := &recordingTarget{}
	r, err := NewRelay(RelayCfg{}, pool, target, tp, nil, m, log)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	// Force the poll loop to fetch a new source connection repeatedly.
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		select {
		case r.signalCh <- struct{}{}:
		default:
		}
	}
	time.Sleep(200 * time.Millisecond)

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Allow closed clients' goroutines to exit.
	deadline := time.Now().Add(3 * time.Second)
	for clientGoroutines() > baseline && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := clientGoroutines(); n > baseline {
		t.Fatalf("%d client goroutines after shutdown, want at most %d; source clients were not closed", n, baseline)
	}
}
