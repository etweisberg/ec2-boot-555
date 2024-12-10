#!/bin/bash

# Check if an argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <run_python>"
    echo "<run_python> should be 'yes' to run the Python script or 'no' to skip it."
    exit 1
fi

# Load env
source .env

# Update the system
sudo yum update -y

# Install Amazon Corretto 22
echo "Installing Amazon Corretto 22..."
sudo rpm --import https://yum.corretto.aws/corretto.key
sudo curl -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
sudo yum install -y java-22-amazon-corretto-devel

# Install python3 and pip
echo "Installing Python 3 and pip..."
sudo yum install -y python3 python3-pip

# Verify installations
echo "Verifying installations..."
java -version
python3 --version
pip3 --version

# Create the w1 directory
echo "Creating directory 'w1'..."
mkdir -p w1

# Check if the Python script should be run
if [ "$1" == "yes" ]; then
    echo "Running s3.py script..."
    python3 s3.py w1 -d --max_threads 32
else
    echo "Skipping the execution of the Python script."
fi

echo "Script execution completed."

