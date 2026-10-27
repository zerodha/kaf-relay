package relay

import "context"

// Header is a key-value pair attached to a Message.
type Header struct {
	Key   string
	Value []byte
}

// SourcePartition as a Message.Partition asks the target to write to the partition the message was read from.
const SourcePartition int32 = -1

// Message is a single Kafka message flowing through the relay pipeline.
// The relay converts source Kafka records into Message{}s before
// passing them to a Target.
type Message struct {
	Key             []byte
	Value           []byte
	Headers         []Header
	Topic           string // Target topic
	Partition       int32  // Target partition, or SourcePartition to mirror the source.
	Offset          int64  // Source message offset.
	SourcePartition int32  // Source partition (for offset tracking by targets).
}

// Offsets maps topic -> partition -> offset. This represents the progress of message offsets at a target.
type Offsets map[string]map[int32]int64

// Target is the interface for a relay target/destination. The bundled `kafkatarget` package implements this for Kafka.
// This interface can be implemented to relay messages to other systems (Redis, HTTP, etc.).
type Target interface {
	// GetHighWatermark returns, per topic and source partition, the next source offset to
	// consume (the last written Message.Offset + 1). The relay resumes from these offsets and
	// consumes partitions that aren't listed from the start.
	// Targets that don't/can't track offsets should return empty Offsets and nil, NOT an error.
	GetHighWatermark(ctx context.Context) (Offsets, error)

	// Start starts the target's background worker that batches and flushes source messages
	// to the target.
	Start() error

	// Write is a blocking function that queues a message for writing to target. Blocking is necessary
	// to wait on source consumption so that the target can catch up. It returns an error if the target is closed.
	Write(ctx context.Context, msg Message) error

	// Close closes the target and waits until all pending messages are flushed.
	Close() error
}

// PartitionCounter is an optional Target interface for targets with Kafka-style partitions.
// When implemented, the relay checks that the source and target partition counts match
// before mirroring source partitions.
type PartitionCounter interface {
	PartitionCount(ctx context.Context, topic string) (int, error)
}
