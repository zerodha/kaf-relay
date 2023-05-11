package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func getCompressionCodec(codec string) kgo.CompressionCodec {
	switch codec {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	case "none":
		return kgo.NoCompression()

	default:
		return kgo.NoCompression()
	}
}

// getAckPolicy generates franz-go's commit ack for the given stream.CommitAck.
func getAckPolicy(ack string) kgo.Acks {
	switch ack {
	case "tcp":
		return kgo.NoAck()
	case "cluster":
		return kgo.AllISRAcks()
	case "leader":
		return kgo.LeaderAck()

	// fallback to default config in franz-go
	default:
		return kgo.AllISRAcks()
	}
}

func testConnection(client *kgo.Client, timeout time.Duration, topics []string) error {
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	req.AllowAutoTopicCreation = false
	req.IncludeClusterAuthorizedOperations = true
	req.IncludeTopicAuthorizedOperations = false

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	// Verify error code from requested topics response
	for _, topic := range resp.Topics {
		err = kerr.ErrorForCode(topic.ErrorCode)
		if err != nil {
			return err
		}
	}

	// force refresh client metadata info
	client.ForceMetadataRefresh()

	return nil
}
