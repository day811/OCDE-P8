#!/bin/bash
# aws-deploy/create-state-machine-final.sh
# Crée la State Machine (VERSION FINAL)

set -e

# Charger les variables
if [ ! -f ../.env.ecs ]; then
    echo "❌ Erreur : .env.ecs non trouvé"
    exit 1
fi

export $(cat ../.env.ecs | grep -v '^#' | xargs)

echo "============================================================================"
echo "📋 CRÉATION STATE MACHINE (VERSION FINAL)"
echo "============================================================================"
echo ""

# Lire le fichier JSON et l'echapper
DEFINITION=$(cat pipeline-state-machine.json) 

# Créer avec chaîne JSON (pas file://)
aws stepfunctions create-state-machine \
  --name greencoop-pipeline \
  --definition file://json-policies/pipeline-state-machine.json \
  --role-arn "arn:aws:iam::$AWS_ACCOUNT_ID:role/StepFunctionsExecutionRole" \
  --region $AWS_REGION

if [ $? -eq 0 ]; then
    echo "✅ State Machine créée avec succès"
else
    echo "❌ Erreur lors de la création"
    exit 1
fi
