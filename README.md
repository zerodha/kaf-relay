## Relay

Relay is a Golang program designed to forward topics from one Kafka cluster to another Kafka cluster. It provides features such as plain authentication, topic remapping, and utilizes consumer groups to efficiently consume a list of Kafka topics for forwarding.

### Features

* Topic Forwarding: Relay consumes messages from topics in one Kafka cluster and forwards them to topics in another Kafka cluster.
* Plain Authentication: Relay supports plain authentication for connecting to both the source and destination Kafka clusters.
* Topic Remapping: Relay allows you to specify topic remappings, enabling you to map a topic from the source cluster to a different topic in the destination cluster.

## Prerequisites

* Go 1.15 or higher installed
* Access to the source and destination Kafka clusters

## Installation

```bash
git clone https://github.com/zerodha/relay.git
cd relay
make dist
```

## Usage

To run Relay, follow these steps:

Create a configuration file named config.toml with the necessary settings. You can use the provided config.example.toml file as a template.

Execute the Relay binary using the following command:

```bash
./relay.bin --config config.toml
```
