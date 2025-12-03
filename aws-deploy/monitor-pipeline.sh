#!/bin/bash
# aws-deploy/monitor-pipeline.sh
# Monitore l'exécution du pipeline

set -e

# Charger les variables
if [ ! -f ../.env.ecs ]; then
    echo "❌ Erreur : .env.ecs non trouvé"
    exit 1
fi

export $(cat ../.env.ecs | grep -v '^#' | xargs)

# Récupérer l'ARN de la dernière exécution
echo "🔍 Recherche exécution en cours..."

# Récupérer sans --sort-order (pas supporté)
EXECUTION_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:$AWS_REGION:$AWS_ACCOUNT_ID:stateMachine:greencoop-pipeline" \
  --region $AWS_REGION \
  --query 'executions[0].executionArn' \
  --output text 2>/dev/null || echo "")

if [ -z "$EXECUTION_ARN" ] || [ "$EXECUTION_ARN" = "None" ]; then
    echo "❌ Aucune exécution trouvée"
    echo ""
    echo "Lancez d'abord le pipeline :"
    echo "   bash run-pipeline.sh"
    exit 1
fi

echo "📊 Execution ARN: $EXECUTION_ARN"
echo ""
echo "🔄 Monitoring en cours... (Ctrl+C pour arrêter)"
echo ""

# Boucle de monitoring
COUNTER=0
while true; do
    # Récupérer le status
    STATUS=$(aws stepfunctions describe-execution \
      --execution-arn "$EXECUTION_ARN" \
      --region $AWS_REGION \
      --query 'status' \
      --output text)
    
    # Récupérer le timestamp de mise à jour
    TIMESTAMP=$(date '+%H:%M:%S')
    
    echo "[$TIMESTAMP] Status: $STATUS"
    
    # Quitter si terminé
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo ""
        echo "✅ Pipeline RÉUSSI"
        echo ""
        echo "📊 Détails:"
        aws stepfunctions describe-execution \
          --execution-arn "$EXECUTION_ARN" \
          --region $AWS_REGION \
          --query '{Status: status, StartDate: startDate, StopDate: stopDate}' \
          --output table
        break
    elif [ "$STATUS" = "FAILED" ]; then
        echo ""
        echo "❌ Pipeline ÉCHOUÉ"
        
        # Afficher l'erreur
        ERROR_INFO=$(aws stepfunctions describe-execution \
          --execution-arn "$EXECUTION_ARN" \
          --region $AWS_REGION \
          --query '{Status: status, Cause: cause}' \
          --output json)
        
        echo "$ERROR_INFO"
        break
    elif [ "$STATUS" = "TIMED_OUT" ]; then
        echo ""
        echo "⏱️  Pipeline TIMEOUT"
        break
    elif [ "$STATUS" = "ABORTED" ]; then
        echo ""
        echo "🛑 Pipeline INTERROMPU"
        break
    fi
    
    sleep 5
    COUNTER=$((COUNTER + 1))
    
    # Max 1 hour = 720 iterations
    if [ $COUNTER -gt 720 ]; then
        echo ""
        echo "⏱️  Timeout: Pipeline a pris plus de 1 heure"
        break
    fi
done
