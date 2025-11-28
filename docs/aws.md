UUID airbyte059005f6-5858-48da-bf49-c00322f2390e

Policy for airbyte-service-group

Excellente question ! C'est une préoccupation légitime de **gestion du stockage et des coûts S3**. Voici la bonne pratique pour ce type de pipeline :

***

## Architecture Recommandée : S3 comme "Staging Area"

```
Airbyte (Source APIs)
        ↓
        ├─→ S3 (Raw Data - Staging)
        │   └─→ Conservé temporairement
        │
        ├─→ Transformation Python
        │   └─→ Extraction, nettoyage, enrichissement
        │
        └─→ MongoDB (Data Warehouse)
            └─→ Données transformées et consolidées
```

***

## Stratégie de Nettoyage S3 : Lifecycle Policy

**Non, vous ne devez PAS supprimer manuellement les données**. Utilisez une **S3 Lifecycle Policy** pour automatiser le nettoyage.

### Configuration de la Lifecycle Policy

```json
{
  "Rules": [
    {
      "Id": "Archive-raw-data-after-90-days",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "infoclimat/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"  # Standard-Infrequent Access (70% moins cher)
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER"  # Archive longue durée (90% moins cher)
        }
      ],
      "Expiration": {
        "Days": 365  # Supprimer après 1 an
      }
    },
    {
      "Id": "Archive-wunderground-data",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "wunderground/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER"
        }
      ],
      "Expiration": {
        "Days": 365
      }
    }
  ]
}
```

***

## Phases de la Lifecycle Policy

| Phase | Jours | Classe de Stockage | Coût | Accès | Utilité |
|-------|-------|-------------------|------|-------|---------|
| **Actif (STANDARD)** | 0-30 | Standard | 100% | Rapide | Données fraîches pour transformation |
| **Archive Froide (STANDARD_IA)** | 30-90 | Standard-IA | ~30% | Lent (mais possible) | Historique récent, si bug de transformation |
| **Archive Très Froide (GLACIER)** | 90-365 | Glacier | ~10% | Très lent (heures) | Conformité légale, audit trail |
| **Suppression** | 365+ | - | 0% | ❌ | Nettoyage final après 1 an |

***

## Mise en Place dans AWS Console

### Étape 1 : Accéder à la Lifecycle Policy

```
AWS Console → S3 → greencoop-forecast-raw-data
→ Management → Lifecycle rules → Create lifecycle rule
```

### Étape 2 : Configuration

1. **Lifecycle rule name** : `archive-raw-data`

2. **Choose a filter** :
   - Sélectionner **Prefix** : `infoclimat/` (ou créer une règle par source)

3. **Lifecycle rule actions** :
   - ☑ Transition current versions of objects between storage classes
   - ☑ Expire current versions of objects

4. **Transitions** :
   - Jour 30 → STANDARD_IA
   - Jour 90 → GLACIER

5. **Expiration** :
   - Jour 365 → Supprimer

6. **Create rule**

***

## Workflow Optimal pour GreenAndCoop

```
Jour 1 : 
├─ Infoclimat arrive via Airbyte → S3 (STANDARD)
├─ Python transformation lit S3 → traite
└─ MongoDB stocke données transformées

Jour 15 :
├─ Infoclimat continue d'arriver → S3 (STANDARD)
├─ Python transformation continue
└─ MongoDB accumule historique

Jour 31 :
├─ Données >30j transférées → STANDARD_IA (coût -70%)
├─ Données récentes restent en STANDARD
└─ MongoDB continue (source unique de vérité)

Jour 91 :
├─ Données >90j transférées → GLACIER (coût -90%)
├─ Données 30-90j restent en STANDARD_IA
└─ MongoDB reste accessiblerapidement

Jour 365 :
├─ Données >1an supprimées automatiquement
├─ Vous avez gardé l'historique 12 mois dans MongoDB
└─ Conformité légale satisfaite
```

***

## Avantages de cette Approche

| Aspect | Bénéfice |
|--------|----------|
| **Coûts** | Réduction jusqu'à 90% après 90 jours |
| **Automatisation** | Zéro intervention manuelle |
| **Auditabilité** | Historique complet en GLACIER (1 an) |
| **Récupération** | Si bug Python = accès aux données brutes |
| **Conformité** | Rétention légale respectée |
| **Maintenance** | Pas de script custom à maintenir |

***

## À NE PAS FAIRE

❌ **Ne pas supprimer manuellement** les fichiers S3  
❌ **Ne pas archiver dans un bucket différent**  
❌ **Ne pas compter sur Python** pour nettoyer S3

✅ **Utiliser une Lifecycle Policy AWS** - c'est fait pour ça

***

## Configuration Python : Lire depuis S3 Pendant Toute la Durée

Votre script Python doit **toujours** pouvoir lire depuis S3, même après archivage en GLACIER :

```python
# transformation/src/s3_reader.py

import boto3
import time

class S3Reader:
    def __init__(self, bucket: str):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
    
    def read_object_with_restore(self, s3_key: str, max_wait_hours: int = 24):
        """
        Lit un objet S3, en attendant si nécessaire la restauration depuis GLACIER.
        """
        try:
            # Tentative de lecture directe
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            return response['Body'].read()
        
        except self.s3_client.exceptions.NoSuchKey:
            print(f"❌ Clé non trouvée : {s3_key}")
            raise
        
        except Exception as e:
            # Si l'objet est en GLACIER, lancer une restauration
            if "InvalidObjectState" in str(e):
                print(f"⏳ Objet en archivage GLACIER, restauration en cours...")
                self.s3_client.restore_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    RestoreRequest={
                        'Days': 7,
                        'GlacierJobParameters': {
                            'Tier': 'Standard'  # Restauration en 3-5 heures
                        }
                    }
                )
                print(f"⏳ Restauration lancée, réessayez dans 3-5 heures")
                raise
            else:
                raise
```

***

## Résumé de la Stratégie

**Question** : "Dois-je supprimer les données S3 après transfert à MongoDB ?"

**Réponse** : 
- ✅ **OUI, automatiquement** via Lifecycle Policy
- ✅ **Jour 30** : STANDARD_IA (archive froide)
- ✅ **Jour 90** : GLACIER (archive très froide)
- ✅ **Jour 365** : Suppression automatique
- ✅ **MongoDB** : Reste accessible en permanence

Cette approche respecte les meilleures pratiques AWS et optimise vos coûts sans intervention manuelle.

Voulez-vous que je vous montre comment configurer la Lifecycle Policy via CLI ou AWS Console ?