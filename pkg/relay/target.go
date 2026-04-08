package relay

import "context"

// Header is a key-value pair attached to a Message.
type Header struct {
	Key   string
	Value []byte
}

// Message is a single Kafka message flowing through the relay pipeline.
// The relay converts source Kafka records into Message{}s before
// passing them to a Target.
type Message struct {
	Key             []byte
	Value           []byte
	Headers         []Header
	Topic           string // Target topic
	Partition       int32  // Target partition (-1 for auto)
	Offset          int64  // Source message offset.
	SourcePartition int32  // Source partition (for offset tracking by targets).
}

// Offsets maps topic -> partition -> offset. This represents the progress of message offsets at a target.
type Offsets map[string]map[int32]int64

// Target is the interface for a relay target/destination. The bundled `kafkatarget` package implements this for Kafka.
// This interface can be implemented to relay messages to other systems (Redis, HTTP, etc.).
type Target interface {
	// GetHighWatermark returns the target's current offsets per topic-partition, which is then
	// used by the relay to resume consumption from the source.
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
