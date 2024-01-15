#!/bin/bash
# Usage of the script
# Bring up mode   : ./chaos_monkey.sh -d /path/to/kafka/bin/dir -m up for starting services that were taken down
# Bring down mode : ./chaos_monkey.sh -d /path/to/kafka/bin/dir -m down for stopping services that were started

# Define an array of server names
consumer_servers=("server.properties" "server1.properties")
producer_servers=("server2.properties")
all_servers=("server.properties" "server1.properties" "server2.properties")

workspace=

# Parse command line options
while getopts ":m:d:" opt; do
  case ${opt} in
    m )
      mode=$OPTARG
      ;;
    d )
      workspace=$OPTARG
      ;;
    \? )
      echo "Invalid option: $OPTARG" 1>&2
      exit 1
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" 1>&2
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

if [ -z "$workspace" ]; then
  echo "Workspace directory is required. Please provide the -d option."
  exit 1
fi

if [ "$mode" == "down" ]; then
    while true; do
      # Randomly select a server
      server=${all_servers[$RANDOM % ${#all_servers[@]}]}

      # Kill the server process
      pid=$(ps aux | grep "[${server:0:1}]${server:1}" | awk '{print $2}')
      # If pid is empty, skip the loop
      if [ -z "$pid" ]; then
        continue
      fi

      echo "$server -> $pid"
      kill -INT $pid 

      # Check if the kill command was successful
      if [ $? -eq 0 ]
      then
        echo "$server down"
      else
        echo "$server already down"
      fi

      # Sleep for a random amount of time between 5 and 10 seconds
      sleep $((RANDOM % 6 + 5))
    done
elif [ "$mode" == "up" ]; then
    while true; do
      for server in "${all_servers[@]}"; do
        # Check if the server process is running
        pid=$(ps aux | grep "[${server:0:1}]${server:1}" | awk '{print $2}')

        if [ -z "$pid" ]; then
          # Restart the server process
          (trap - SIGINT; ${workspace}/bin/kafka-server-start.sh ${workspace}/config/$server > /dev/null 2>&1) &

          # Check if the start command was successful
          if [ $? -eq 0 ]
          then
            echo "Successfully restarted $server"
          else
            echo "Failed to restart $server"
          fi
        fi
      done

      # Sleep for a random amount of time between 1 and 3 seconds
      sleep $((RANDOM % 3 + 1))
    done
fi