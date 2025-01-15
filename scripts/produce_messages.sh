#!/bin/bash

# Configuration variables
TOPIC_NAME="test-topic"                 # Target Kafka topic
NUM_MESSAGES=1000000                    # Number of messages to produce
BATCH_SIZE=10000                         # Number of messages per batch
#BOOTSTRAP_SERVER="localhost:9091"       # Kafka broker address
BOOTSTRAP_SERVER="$1"       # Kafka broker address

# Function to check if Kafka is running
check_kafka() {
    if ! nc -z localhost 9092; then
        echo "Error: Kafka broker is not running on localhost:9092"
        echo "Please start Kafka before running this script"
        exit 1
    fi
}

# Function to create topic if it doesn't exist
create_topic() {
    kaf topic create $TOPIC_NAME
}

# Function to produce messages in batches
produce_messages() {
    local batch_count=$((NUM_MESSAGES / BATCH_SIZE))
    local message_count=0
    
    echo "Starting message production..."
    echo "Total messages to produce: $NUM_MESSAGES"
    echo "Batch size: $BATCH_SIZE"
    echo "Number of batches: $batch_count"
    
    for ((batch=1; batch<=batch_count; batch++)); do
        # Create a temporary file for the batch
        local temp_file=$(mktemp)
        
        # Generate batch of messages
        for ((i=1; i<=BATCH_SIZE; i++)); do
            message_count=$((message_count + 1))
            echo $message_count >> $temp_file
        done
        
        # Send batch to Kafka using kaf
        cat $temp_file | kaf produce $TOPIC_NAME --brokers $BOOTSTRAP_SERVER
        
        # Clean up temp file
        rm $temp_file
        
        # Progress update
        if ((batch % 10 == 0)); then
            echo "Progress: $message_count / $NUM_MESSAGES messages produced"
        fi
    done
    
    echo "Message production completed!"
    echo "Total messages produced: $message_count"
}

# Main execution
echo "Kafka Message Producer Script"
echo "----------------------------"

# Check if kaf is installed
if ! command -v kaf &> /dev/null; then
    echo "Error: kaf is not installed"
    echo "Please install kaf before running this script"
    exit 1
fi

# Check if Kafka is running
check_kafka

# Create topic if it doesn't exist
create_topic

# Start producing messages
produce_messages
