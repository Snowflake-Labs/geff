#!/bin/bash

ACCOUNT_ID="$1"
AWS_REGION="$2"
IMAGE_VER=geff:latest
ECR_HOST="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

# Create repo for GEFF docker image
aws ecr create-repository --repository-name geff --region $AWS_REGION

# NOTE: From here all steps need docker desktop to be running.

# Login
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_HOST

# Docker Build (Needs docker desktop running)
docker build -t geff .

docker tag $IMAGE_VER $ECR_HOST/$IMAGE_VER

docker push $ECR_HOST/$IMAGE_VER
