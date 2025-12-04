#!/bin/bash
set -e
source .env.ecs

# Parse arguments
BUILD=true
SERVICE_FILTER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-build)
            BUILD=false
            shift
            ;;
        --service)
            SERVICE_FILTER="$2"
            shift 2
            ;;
        *)
            echo "Usage: $0 [--no-build] [--service preprocessing|ingestion|quality-checker]"
            exit 1
            ;;
    esac
done

# Login ECR
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

SERVICES=(preprocessing ingestion quality-checker)

for SERVICE in "${SERVICES[@]}"; do
    # Filtrer si --service spécifié
    if [[ -n "$SERVICE_FILTER" && "$SERVICE" != "$SERVICE_FILTER" ]]; then
        continue
    fi
    
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
