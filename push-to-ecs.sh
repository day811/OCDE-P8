#!/bin/bash
set -e

source .env.ecs

# Login ECR
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build et push preprocessing
cd src/preprocessing
docker build -t greencoop/preprocessing:latest .
docker tag greencoop/preprocessing:latest \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/greencoop/preprocessing:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/greencoop/preprocessing:latest
cd ../..

# Build et push ingestion
cd src/ingestion
docker build -t greencoop/ingestion:latest .
docker tag greencoop/ingestion:latest \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/greencoop/ingestion:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/greencoop/ingestion:latest
cd ../..

# Build et push quality-checker
cd src/quality_checker
docker build -t greencoop/quality-checker:latest .
docker tag greencoop/quality-checker:latest \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/greencoop/quality-checker:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/greencoop/quality-checker:latest
cd ../..

echo "✓ Toutes les images pushées vers ECR"