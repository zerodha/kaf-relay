## Relay

Relay is a program designed to forward topics from one Kafka cluster to another Kafka cluster.

### Features

* Topic Forwarding: Relay consumes messages from topics in one Kafka cluster and forwards them to topics in another Kafka cluster.
* Authentication: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
* Topic Remapping: Relay allows you to specify topic remappings, enabling you to map a topic from the source cluster to a different topic in the destination cluster.

## Prerequisites

* Go installed.
* Access to the source and destination Kafka clusters.

## Installation

```bash
git clone https://github.com/zerodha/relay.git
cd relay
make dist
```

## Usage

To run Relay, follow these steps:

Create a configuration file named config.toml with the necessary settings. You can use the provided config.example.toml file as a template.

```bash
./relay.bin --config config.toml
```
