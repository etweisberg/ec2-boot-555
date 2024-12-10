#!/bin/bash

# Ensure PUB_IP is set
if [ -z "$PUB_IP" ]; then
  echo "Error: PUB_IP environment variable is not set."
  exit 1
fi

# Use the public IP for the coordinator
COORDINATOR="$PUB_IP:9000"

# Submit Flame jobs with different seed URLs
for seed_url in "${SEED_URLS[@]}"; do
  echo "Submitting Flame job with seed URL: $seed_url"
  java -cp crawler.jar:flame.jar:kvs.jar:webserver.jar:log4j_lib/*:lucene_lib/* cis5550.flame.FlameSubmit "$COORDINATOR" jobs.jar cis5550.jobs.TfIdfEC2 &
done

# Wait for all background processes to complete
wait