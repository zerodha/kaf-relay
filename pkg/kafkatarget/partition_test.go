package kafkatarget

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

func TestWritePartition(t *testing.T) {
	cases := []struct {
		name            string
		partition       int32
		sourcePartition int32
		want            int32
	}{
		{"mirrors source partition", relay.SourcePartition, 2, 2},
		{"uses explicit partition", 1, 2, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target := &Target{inletCh: make(chan *kgo.Record, 1)}
			if err := target.Write(context.Background(), relay.Message{
				Topic: "target", Partition: c.partition, SourcePartition: c.sourcePartition,
			}); err != nil {
				t.Fatal(err)
			}

			if got := (<-target.inletCh).Partition; got != c.want {
				t.Errorf("record partition = %d, want %d", got, c.want)
			}
		})
	}
}

func TestCloseClosesClient(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "target"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	tp := relay.Topic{SourceTopic: "source", TargetTopic: "target", AutoTargetPartition: true}
	tg, err := New(context.Background(), relay.TargetCfg{ReqTimeout: time.Second}, relay.ProducerCfg{
		KafkaCfg:        relay.KafkaCfg{BootstrapBrokers: c.ListenAddrs(), SessionTimeout: 5 * time.Second},
		MaxRetries:      relay.IndefiniteRetry,
		FlushFrequency:  50 * time.Millisecond,
		MaxMessageBytes: 1 << 20,
		BatchSize:       10,
		BufferSize:      10,
		FlushBatchSize:  10,
	}, relay.Topics{"source": tp}, metrics.NewSet(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	go tg.Start()
	if err := tg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tg.Close(); err != nil {
		t.Fatal(err)
	}

	err = tg.client.ProduceSync(context.Background(), &kgo.Record{Topic: "target"}).FirstErr()
	if !errors.Is(err, kgo.ErrClientClosed) {
		t.Fatalf("produce after Close() = %v, want ErrClientClosed", err)
	}
}
