<a href="https://zerodha.tech"><img src="https://zerodha.tech/static/images/github-badge.svg" align="right" /></a>
## kaf-relay

kaf-relay is an opinionated, high performance program for keeping Kafka clusters in sync by replicating topics. It is specfically designed for high-availability with background healthchecks, offset tracking, and topic lag checks.

### Features

* Topic Forwarding: Relay consumes messages from topics in one Kafka cluster and forwards them to topics in another Kafka cluster.
* Authentication: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
* Topic Remapping: Relay allows you to specify topic remappings, enabling you to map a topic from the source cluster to a different topic in the destination cluster.
* Consumer group failover: Given identical Kafka instances (separate nodes 1...N) at the upstream, instantly switch over to the next node in a round-robin fashion on current node failure. Offset tracking between source and target nodes allows de-duplication without external stores.
* Topic lag failover: Monitors offsets amongst N identical nodes to detect lags and to instantly switch upstream consumer nodes.
* Stop at end: Flag `--stop-at-end` allows the program to stop after reaching the end of consumer topic offsets that was picked up on boot.
* Filter messages using go plugins: Flag `--filter` allows the program to filter messages based on the logic in plugin code.

#### kaf-relay in different modes

![image](./screenshots/relay.png)


## Usage

To run kaf-relay, follow these steps:

Copy config.sample.toml to config.toml and edit it accordingly. Then run:

```bash
./kaf-relay.bin --config config.toml --mode <single/failover>
```

### Filter plugins

Plugins allow to parse and filter incoming messages to allow or deny them from being replicated downstream. Build your own filter plugins by implementing `filter.Provider` interface.

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
