#!/bin/bash
# aws-deploy/generate-task-definitions.sh
# Génère toutes les task definitions finales depuis les templates

set -e

# Charger les variables
if [ ! -f ../.env.ecs ]; then
    echo "❌ Erreur : .env.ecs non trouvé"
    exit 1
fi

export $(cat ../.env.ecs | grep -v '^#' | xargs)

echo "📋 Génération des Task Definitions..."
echo ""

# Générer chaque template
for template in json-policies/*-task-def.template.json json-policies/pipeline-state-machine.template.json; do
    output="json-policies/${template%.template.json}.json"
    
    echo "🔄 Génération : $output"
    envsubst < "$template" > "$output"
    
    if [ $? -eq 0 ]; then
        echo "✅ $output généré"
    else
        echo "❌ Erreur: $output"
        exit 1
    fi
done

echo ""
echo "============================================================================"
echo "✅ TOUTES LES TASK DEFINITIONS GÉNÉRÉES"
echo "============================================================================"
echo ""
echo "⏭️  Prochaine étape :"
echo "   cd aws-deploy && bash register-task-definitions.sh"
