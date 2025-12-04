#!/bin/bash
set -e
source .env.ecs

# Parse arguments
BUILD=true
if [[ "$1" == "--no-build" ]]; then
    BUILD=false
fi

# Login ECR
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

for SERVICE in preprocessing ingestion quality-checker; do
    IMAGE_NAME="greencoop/$SERVICE"
    ECR_URI="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$IMAGE_NAME:latest"
    DOCKERFILE="src/${SERVICE//-/_}/Dockerfile"
    
    if [[ "$BUILD" == true ]]; then
        echo "🔨 Build $SERVICE..."
        docker build -t $IMAGE_NAME:latest -f $DOCKERFILE .
    fi
    
    echo "🏷️  Tag $SERVICE..."
    docker tag $IMAGE_NAME:latest $ECR_URI
    
    echo "📤 Push $SERVICE..."
    docker push $ECR_URI
done

echo "✓ Terminé"
