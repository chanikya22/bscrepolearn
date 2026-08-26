#!/bin/bash

# Enable error handling
set -e

# Define variables
DOCKERFILE="Dockerfile"
DOCKER_IMAGE_NAME="insytflow-image"
DOCKER_COMPOSE_FILE="docker-compose.yaml"

# Check if Dockerfile exists
if [ ! -f "$DOCKERFILE" ]; then
  echo "Error: $DOCKERFILE not found in the current directory."
  exit 1
fi

# Build Docker image
echo "Building Docker image from $DOCKERFILE..."
docker build -t "$DOCKER_IMAGE_NAME" .

# Check if docker-compose.yaml exists
if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
  echo "Error: $DOCKER_COMPOSE_FILE not found in the current directory."
  exit 1
fi

# Run docker-compose
echo "Starting services with $DOCKER_COMPOSE_FILE..."
docker-compose up -d

#
