#!/bin/bash

# Directory to monitor
CRAWL_DIR="w1/pt-crawl/"

# Interval (in seconds) between checks
INTERVAL=60

# File to store the last count
LAST_COUNT_FILE=".last_count"

# Initialize the last count file if it doesn't exist
if [[ ! -f $LAST_COUNT_FILE ]]; then
    echo "0" > $LAST_COUNT_FILE
fi

# Start monitoring
echo "Monitoring the number of URLs crawled in '$CRAWL_DIR' every $INTERVAL seconds."
echo "Press Ctrl+C to stop."

while true; do
    # Get the current count of URLs
    CURRENT_COUNT=$(ls "$CRAWL_DIR" -l | wc -l)

    # Read the last count from the file
    LAST_COUNT=$(cat $LAST_COUNT_FILE)

    # Calculate the number of new URLs crawled
    NEW_URLS=$((CURRENT_COUNT - LAST_COUNT))

    # Output the result
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
    echo "$TIMESTAMP - URLs Crawled in the Last $INTERVAL Seconds: $NEW_URLS"

    # Update the last count
    echo "$CURRENT_COUNT" > $LAST_COUNT_FILE

    # Wait for the next interval
    sleep $INTERVAL
done

