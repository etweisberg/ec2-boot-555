#!/bin/bash

# Ensure PUB_IP is set
if [ -z "$PUB_IP" ]; then
  echo "Error: PUB_IP environment variable is not set."
  exit 1
fi

# Use the public IP for the coordinator
COORDINATOR="$PUB_IP:9000"

# Submit Flame jobs with different seed URLs
echo "Submitting Flame job: $seed_url"
java -Xms24g -Xmx24g -cp crawler.jar:flame.jar:kvs.jar:webserver.jar:log4j_lib/*:lucene_lib/*:tika_lib/* cis5550.flame.FlameSubmit "$COORDINATOR" jobs.jar cis5550.jobs.TfIdfEC2 &


# Wait for all background processes to complete
wait
