<a href="https://zerodha.tech"><img src="https://zerodha.tech/static/images/github-badge.svg" align="right" /></a>
## Relay

Relay is an opinionated program designed to replicate messages on topics from one Kafka cluster to another Kafka cluster.

### Features

* Topic Forwarding: Relay consumes messages from topics in one Kafka cluster and forwards them to topics in another Kafka cluster.
* Authentication: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
* Topic Remapping: Relay allows you to specify topic remappings, enabling you to map a topic from the source cluster to a different topic in the destination cluster.
* Consumer group failover: Assuming we have multiple identical kafkas (separate nodes 1...N) at the upstream side, this mode allows us to fallback to the next kafka in a round-robin fashion if current broker goes down. This allows us to deduplicate messages downstream without using any external stores.
* Topic lag failover: In addition to consumer group failover, if there is a lag between node1 and node2, we initiate an immediate switch-over to node2.
* Stop at end: Flag `--stop-at-end` allows the program to stop after reaching the end of consumer topic offsets that was picked up on boot.
* Filter messages using go plugins: Flag `--filter` allows the program to filter messages based on the logic in plugin code.

#### relay in different modes

![image](./screenshots/relay.png)

## Prerequisites

* Go installed.
* Access to the source and destination Kafka clusters.

## Installation

```bash
git clone https://github.com/joeirimpan/kaf-relay.git
cd kaf-relay
make dist
```

## Usage

To run Relay, follow these steps:

Create a configuration file named config.toml with the necessary settings. You can use the provided config.example.toml file as a template.

```bash
./kaf-relay.bin --config config.toml --mode <single/failover>
```

### Filter plugins

Build your own filter plugins by implementing `filter.Provider` interface.

Sample
```golang
package main

import (
	"encoding/json"
)

type TestFilter struct {
}

type Config struct {
}

func New(b []byte) (interface{}, error) {
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}

	return &TestFilter{}, nil
}

func (f *TestFilter) ID() string {
	return "testfilter"
}

func (f *TestFilter) IsAllowed(msg []byte) bool {
	return false
}
```
* Copy this plugin code to a directory. `mkdir testfilter && cp sample.go testfilter`
* Build the plugin. `CGO_ENABLED=1 go build -a -ldflags="-s -w" -buildmode=plugin -o testfilter.filter sample.go`
* Change the config.toml to add the filter provider config.
* Run kaf-relay with the filter pluing. `./kaf-relay.bin --mode single --stop-at-end --filter ./testfilter/testfilter.filter`

## Metrics

Replication metrics are exposed through a HTTP server.

```
$ curl localhost:7081/metrics
kafka_relay_msg_count{source="topicA", destination="machineX_topicA", partition="0"} 44
kafka_relay_msg_count{source="topicA", destination="machineX_topicA", partition="1"} 100
kafka_relay_msg_count{source="topicA", destination="machineX_topicA", partition="2"} 100
kafka_relay_msg_count{source="topicA", destination="machineX_topicA", partition="3"} 44
kafka_relay_msg_count{source="topicA", destination="machineX_topicA", partition="4"} 44
kafka_relay_msg_count{source="topicA", destination="machineX_topicA", partition="5"} 100
```