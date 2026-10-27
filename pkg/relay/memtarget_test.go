package relay_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/zerodha/kaf-relay/pkg/relay"
)

// memTarget is a non-Kafka Target that stores messages in memory and tracks the
// next offset to consume per source partition, like a Redis or DB target would.
type memTarget struct {
	topic string

	mu      sync.Mutex
	offsets relay.TopicOffsets
	msgs    map[int32][]string
}

func newMemTarget(topic string) *memTarget {
	return &memTarget{topic: topic, offsets: relay.TopicOffsets{}, msgs: map[int32][]string{}}
}

func (m *memTarget) GetHighWatermark(context.Context) (relay.Offsets, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(relay.TopicOffsets, len(m.offsets))
	for p, o := range m.offsets {
		out[p] = o
	}
	return relay.Offsets{m.topic: out}, nil
}

func (m *memTarget) Start() error { return nil }
func (m *memTarget) Close() error { return nil }

func (m *memTarget) Write(_ context.Context, msg relay.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.msgs[msg.SourcePartition] = append(m.msgs[msg.SourcePartition], string(msg.Value))
	m.offsets[msg.SourcePartition] = msg.Offset + 1
	return nil
}

func (m *memTarget) received() map[int32][]string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(map[int32][]string, len(m.msgs))
	for p, v := range m.msgs {
		out[p] = append([]string(nil), v...)
	}
	return out
}

func TestMemTargetFreshStart(t *testing.T) {
	const topic = "orders"
	src := newCluster(t, topic, 3)

	want := map[int32][]string{0: values("p0", 3), 1: values("p1", 2), 2: values("p2", 4)}
	for p, v := range want {
		produce(t, src, topic, p, v...)
	}

	target := newMemTarget(topic)
	startRelay(t, []*kfake.Cluster{src}, target, relay.Topic{SourceTopic: topic, TargetTopic: topic, AutoTargetPartition: true})
	waitFor(t, func() bool { return count(target.received()) >= 9 })

	if got := target.received(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("target received %v, want %v", got, want)
	}
}

func TestMemTargetResume(t *testing.T) {
	const topic = "orders"
	src := newCluster(t, topic, 3)
	tp := relay.Topic{SourceTopic: topic, TargetTopic: topic, AutoTargetPartition: true}

	want := map[int32][]string{0: values("first", 3)}
	produce(t, src, topic, 0, want[0]...)

	target := newMemTarget(topic)
	stop := startRelay(t, []*kfake.Cluster{src}, target, tp)
	waitFor(t, func() bool { return count(target.received()) >= 3 })
	stop()

	// Restart with the offsets the target stored. Partition 0 resumes, and partitions
	// the target has never written to must still be consumed.
	for p, v := range map[int32][]string{0: values("p0", 2), 1: values("p1", 2)} {
		produce(t, src, topic, p, v...)
		want[p] = append(want[p], v...)
	}

	startRelay(t, []*kfake.Cluster{src}, target, tp)
	waitFor(t, func() bool { return count(target.received()) >= 7 })

	if got := target.received(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("target received %v, want %v", got, want)
	}
}
