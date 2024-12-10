#!/bin/bash

# Ensure PUB_IP is set
if [ -z "$PUB_IP" ]; then
  echo "Error: PUB_IP environment variable is not set."
  exit 1
fi

# Use the public IP for the coordinator
COORDINATOR="$PUB_IP:8000"

# Start the workers with the public IP
java -Xms24g -Xmx24g -cp kvs.jar:webserver.jar:log4j_lib/*:lucene_lib/*:tika_lib/* cis5550.kvs.Worker 8001 ./w1 $COORDINATOR &

# Optional: Wait for all background processes to complete
wait