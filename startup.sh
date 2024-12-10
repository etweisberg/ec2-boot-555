#!/bin/bash

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

# Load env variables
source .env

# Install Python dependencies
pip install -r requirements.txt

# Run the Python script
echo "Running s3.py script..."
python3 s3.py w1 -d --max_threads 32

echo "Script execution completed."

