#!/bin/bash

# Ensure PUB_IP is set
if [ -z "$PUB_IP" ]; then
  echo "Error: PUB_IP environment variable is not set."
  exit 1
fi

# Use the public IP for the coordinator
COORDINATOR="$PUB_IP:9000"

# List of seed URLs
SEED_URLS=(
    "https://www.wikipedia.org/"
    "https://www.bbc.com/"
    "https://www.cnn.com/"
    "https://news.ycombinator.com/"
    "https://www.stackoverflow.com/"
    "https://www.nytimes.com/"
    "https://www.medium.com/"
)

# Submit Flame jobs with different seed URLs
for seed_url in "${SEED_URLS[@]}"; do
  echo "Submitting Flame job with seed URL: $seed_url"
  java -cp crawler.jar:flame.jar:kvs.jar:webserver.jar:log4j_lib/* cis5550.flame.FlameSubmit "$COORDINATOR" crawler.jar cis5550.jobs.Crawler "$seed_url" &
done

# Wait for all background processes to complete
wait
