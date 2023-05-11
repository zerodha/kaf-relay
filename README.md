<a href="https://zerodha.tech"><img src="https://zerodha.tech/static/images/github-badge.svg" align="right" /></a>
## Relay

Relay is a program designed to replicate messages on topics from one Kafka cluster to another Kafka cluster.

### Features

* Topic Forwarding: Relay consumes messages from topics in one Kafka cluster and forwards them to topics in another Kafka cluster.
* Authentication: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
* Topic Remapping: Relay allows you to specify topic remappings, enabling you to map a topic from the source cluster to a different topic in the destination cluster.

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
./kaf-relay.bin --config config.toml
```

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