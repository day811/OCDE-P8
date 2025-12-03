#!/bin/bash
# aws-deploy/register-task-definitions.sh
# Enregistre toutes les task definitions dans ECS

set -e
set -o pipefail  # ← Important: capture erreurs dans les pipes

# Charger les variables
if [ ! -f ../.env.ecs ]; then
    echo "❌ Erreur : .env.ecs non trouvé"
    exit 1
fi

export $(cat ../.env.ecs | grep -v '^#' | xargs)

echo "📋 Enregistrement des Task Definitions..."
echo ""

# Array des task definitions à enregistrer
TASKDEFS=(
    "json-policies/mongodb-task-def.json"
    "json-policies/preprocessing-task-def.json"
    "json-policies/ingestion-task-def.json"
    "json-policies/quality-checker-task-def.json"
)

# Enregistrer chaque task definition
for taskdef in "${TASKDEFS[@]}"; do
    echo "🔄 Enregistrement : $taskdef"
    
    if [ ! -f "$taskdef" ]; then
        echo "❌ Erreur: Fichier $taskdef non trouvé"
        exit 1
    fi
    
    # Redirectionner la sortie JSON vers /dev/null (pas de flood console)
    if aws ecs register-task-definition \
        --cli-input-json file://$taskdef \
        --region $AWS_REGION > /dev/null 2>&1; then
        echo "✅ $taskdef enregistrée"
    else
        echo "❌ Erreur lors de l'enregistrement de $taskdef"
        echo "   Vérifier AWS CLI, région, fichier JSON"
        exit 1
    fi
    
    echo ""
done

echo "============================================================================"
echo "✅ TOUTES LES TASK DEFINITIONS ENREGISTRÉES (4/4)"
echo "============================================================================"
echo ""

# Créer la State Machine
echo "🔄 Création de la State Machine..."
echo ""

if [ ! -f "json-policies/pipeline-state-machine.json" ]; then
    echo "❌ Erreur: Fichier pipeline-state-machine.json non trouvé"
    exit 1
fi

if aws stepfunctions create-state-machine \
  --name greencoop-pipeline \
  --definition file://json-policies/pipeline-state-machine.json \
  --role-arn arn:aws:iam::$AWS_ACCOUNT_ID:role/StepFunctionsExecutionRole \
  --region $AWS_REGION > /dev/null 2>&1; then
    echo "✅ State Machine créée"
else
    echo "❌ Erreur: State Machine"
    echo "   Vérifier rôle IAM StepFunctionsExecutionRole"
    exit 1
fi

echo ""
echo "============================================================================"
echo "✅ DÉPLOIEMENT COMPLET"
echo "============================================================================"
echo ""
echo "📊 Résumé :"
echo "   - 4 Task Definitions enregistrées"
echo "   - 1 State Machine créée"
echo ""
echo "⏭️  Prochaine étape :"
echo "   aws stepfunctions start-execution --state-machine-arn <ARN> --region eu-west-3"
