#!/bin/bash

# Ensure PUB_IP is set
if [ -z "$PUB_IP" ]; then
  echo "Error: PUB_IP environment variable is not set."
  exit 1
fi

# Use the public IP for the coordinator
COORDINATOR="$PUB_IP:9000"

# Start 100 workers
for ((i=1; i<=8; i++)); do
  PORT=$((9000 + i))
  echo "Starting Worker on port $PORT..."
  java -cp kvs.jar:flame.jar:webserver.jar:log4j_lib/*:lucene_lib/* cis5550.flame.Worker $PORT $COORDINATOR &
done

# Wait for all background processes to finish (optional)
wait
