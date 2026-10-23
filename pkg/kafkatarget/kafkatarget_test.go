package kafkatarget

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

func TestMixedPartitionerHonorsExplicitPartition(t *testing.T) {
	partitioner := mixedPartitioner{automatic: kgo.StickyKeyPartitioner(nil)}
	topicPartitioner := partitioner.ForTopic("topic")

	record := &kgo.Record{Partition: 3}
	if got := topicPartitioner.Partition(record, 5); got != 3 {
		t.Fatalf("Partition() = %d, want explicit partition 3", got)
	}
}

func TestMixedPartitionerChoosesAutomaticPartition(t *testing.T) {
	partitioner := mixedPartitioner{automatic: kgo.StickyKeyPartitioner(nil)}
	topicPartitioner := partitioner.ForTopic("topic")

	record := &kgo.Record{Partition: -1}
	got := topicPartitioner.Partition(record, 5)
	if got < 0 || got >= 5 {
		t.Fatalf("Partition() = %d, want a partition in [0, 5)", got)
	}
}

func TestWriteMarksAutomaticPartitionWithSentinel(t *testing.T) {
	target := &Target{
		inletCh:   make(chan *kgo.Record, 1),
		closeCh:   make(chan struct{}),
		startedCh: make(chan struct{}),
		started:   true,
	}
	close(target.startedCh)

	if err := target.Write(context.Background(), relay.Message{
		Topic:     "topic",
		Partition: -1,
	}); err != nil {
		t.Fatal(err)
	}

	record := <-target.inletCh
	if record.Partition != -1 {
		t.Fatalf("Write() partition = %d, want -1 automatic sentinel", record.Partition)
	}
}

func TestFlushReturnsWhenContextIsCanceled(t *testing.T) {
	target := &Target{
		cfg: relay.TargetCfg{
			ReqTimeout:    time.Second,
			EnableBackoff: true,
			BackoffMin:    time.Millisecond,
			BackoffMax:    time.Millisecond,
		},
		pCfg: relay.ProducerCfg{
			KafkaCfg: relay.KafkaCfg{
				BootstrapBrokers: []string{"127.0.0.1:1"},
				SessionTimeout:   time.Second,
			},
			MaxRetries: relay.IndefiniteRetry,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := target.flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush() error = %v, want context.Canceled", err)
	}
}

func TestCloseBeforeStartReturns(t *testing.T) {
	target := &Target{
		inletCh: make(chan *kgo.Record, 1),
		doneCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
	}

	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteBeforeStartIsRejected(t *testing.T) {
	target := &Target{
		inletCh:   make(chan *kgo.Record, 1),
		closeCh:   make(chan struct{}),
		startedCh: make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	if err := target.Write(context.Background(), relay.Message{Topic: "topic"}); !errors.Is(err, errTargetClosed) {
		t.Fatalf("Write() error = %v, want target-closed error", err)
	}
}
