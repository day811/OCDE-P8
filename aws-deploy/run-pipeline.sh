#!/bin/bash
# aws-deploy/run-pipeline.sh
# Lance l'exécution du pipeline et affiche l'ARN

set -e

# Charger les variables
if [ ! -f ../.env.ecs ]; then
    echo "❌ Erreur : .env.ecs non trouvé"
    exit 1
fi

export $(cat ../.env.ecs | grep -v '^#' | xargs)

echo "============================================================================"
echo "🚀 LANCEMENT PIPELINE GREENCOOP"
echo "============================================================================"
echo ""

# Récupérer l'ARN
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
  --region $AWS_REGION \
  --query 'stateMachines[?name==`greencoop-pipeline`].stateMachineArn' \
  --output text)

if [ -z "$STATE_MACHINE_ARN" ]; then
    echo "❌ State Machine not found"
    exit 1
fi

echo "📍 State Machine: $STATE_MACHINE_ARN"
echo ""

# Lancer l'exécution
RESPONSE=$(aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --region $AWS_REGION)

EXECUTION_ARN=$(echo $RESPONSE | jq -r '.executionArn')
START_DATE=$(echo $RESPONSE | jq -r '.startDate')

echo "✅ Pipeline lancé"
echo ""
echo "📊 Détails :"
echo "   Execution ARN: $EXECUTION_ARN"
echo "   Started at: $START_DATE"
echo ""
echo "⏭️  Prochaine étape :"
echo "   Monitorer avec:"
echo "   bash monitor-pipeline.sh"