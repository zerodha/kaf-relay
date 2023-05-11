package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// relay represents the input, output kafka and the remapping necessary to forward messages from one topic to another.
type relay struct {
	consumer consumer
	producer producer

	topics map[string]string
}

// Start starts the consumer loop on kafka (A), fetch messages and relays over to kafka (B) using an async producer.
func (r *relay) Start(ctx context.Context) {
	r.validateOffsets(ctx)

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			fetches := r.consumer.client.PollFetches(ctx)

			// Proxy fetch errors into error callback function
			errors := fetches.Errors()
			for _, err := range errors {
				log.Printf("error consuming: %v", err.Err)
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()

				// Fetch destination topic. Ignore if remapping is not defined.
				t, ok := r.topics[rec.Topic]
				if !ok {
					continue
				}

				//log.Printf("record remapped topic: %v -> %v; offset %v; key %v\n", rec.Topic, t, rec.Offset, string(rec.Key))

				forward := &kgo.Record{
					Key:   rec.Key,
					Value: rec.Value,
					Topic: t, // remap destination topic
				}

				// manual balancer - assign partition manually
				if r.producer.manualBalancer {
					forward.Partition = rec.Partition
				}

				// TODO: Channel? async works there is a batching behind the scenes?
				r.producer.client.Produce(ctx, forward, func(r *kgo.Record, err error) {
					if err != nil {
						log.Printf("error producing message: %v", err)
					}

					//log.Printf("pushed message: topic %v; offset %v\n", r.Topic, r.Offset)
				})
			}
		}
	}
}

func (r *relay) validateOffsets(ctx context.Context) {
	var (
		consTopics []string
		prodTopics []string
	)
	for c, p := range r.topics {
		consTopics = append(consTopics, c)
		prodTopics = append(prodTopics, p)
	}

	consOffsets := getEndOffsets(ctx, r.consumer.client, consTopics)
	prodOffsets := getEndOffsets(ctx, r.producer.client, prodTopics)

	commitOffsets := getCommittedOffsets(ctx, r.consumer.client, consTopics)

	consOffsets.Each(func(lo kadm.ListedOffset) {
		// Check if mapping exists
		t, ok := r.topics[lo.Topic]
		if !ok {
			log.Fatalf("error finding destination topic for %v in given mapping", lo.Topic)
		}

		// Check if topic, partition exists in destination
		destOffset, ok := prodOffsets.Lookup(t, lo.Partition)
		if !ok {
			log.Fatalf("error finding destination topic, partition for %v in destination kafka", lo.Topic)
		}

		// Confirm that committed offsets of consumer group matches the offsets of destination kafka topic partition
		if destOffset.Offset > 0 {
			o, ok := commitOffsets.Lookup(lo.Topic, destOffset.Partition)
			if !ok {
				log.Fatalf("error finding topic, partition for %v in source kafka", lo.Topic)
			}

			if destOffset.Offset > o.Offset {
				log.Fatalf("destination topic(%v), partition(%v) offsets(%v) is higher than consumer group committed offsets(%v).",
					destOffset.Topic, destOffset.Partition, destOffset.Offset, o.Offset)
			}
		}
	})
}

func getCommittedOffsets(ctx context.Context, client *kgo.Client, topics []string) kadm.ListedOffsets {
	adm := kadm.NewClient(client)
	offsets, err := adm.ListCommittedOffsets(ctx, topics...)
	if err != nil {
		log.Fatalf("error listing committed offsets of topics(%v): %v", topics, err)
	}

	return offsets
}

func getEndOffsets(ctx context.Context, client *kgo.Client, topics []string) kadm.ListedOffsets {
	adm := kadm.NewClient(client)
	offsets, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		log.Fatalf("error listing end offsets of topics(%v): %v", topics, err)
	}

	return offsets
}
