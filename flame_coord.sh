#!/bin/bash

# Check if an IP argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <IP_ADDRESS>"
  exit 1
fi

# Assign the IP address argument to a variable
IP=$1

# Run the Java command with the provided IP address
java -Xms24g -Xmx24g -cp kvs.jar:flame.jar:webserver.jar:log4j_lib/*:lucene_lib/* cis5550.flame.Coordinator 9000 $IP:8000
