# Explication démo

## Vue d'ensemble : Architecture et flux de données

Airbyte est une plateforme d'**intégration de données** (ETL/ELT) qui automatise la capture, la transformation et le chargement de données entre sources et destinations. Le code teste un **connecteur source** (source-faker) qui génère des données synthétiques pour validation.

***

## Analyse du code ligne par ligne

### 1. Initialisation de la source

```python
source = ab.get_source(
    "source-faker",
    config={"count": 5_000},
    install_if_missing=True,
)
```

**Concept : Source et connecteur**

- **Source** : représente un système d'origine de données (base de données, API, fichier, etc.).
- **Connecteur** : implémentation logicielle spécifique pour communiquer avec une source (ici, `source-faker` génère des données fictives).
- **Configuration** : paramètres du connecteur. Ici, `count=5_000` ordonne au connecteur faker de générer 5 000 enregistrements.
- **`install_if_missing=True`** : si le connecteur n'existe pas localement, Airbyte le télécharge et l'installe automatiquement depuis le registry.

---

### 2. Vérification de la connexion

```python
source.check()
```

**Concept : Test de connectivité**

- Valide que la source est accessible et correctement configurée.
- Dans le cas du faker, ce test s'exécute rapidement (il n'y a pas de vraie source distante).
- Sur une vraie base de données, cette étape testerait les identifiants, la connectivité réseau, etc.
- Lève une exception si la configuration est invalide.

***

### 3. Sélection des flux de données

```python
source.select_all_streams()
```

**Concept : Stream et sélection des données**

- **Stream** : flux logique de données, généralement équivalent à une table, une collection MongoDB, des objets d'une API, etc.
- Chaque source expose un ou plusieurs streams (par exemple, une base de données expose ses tables comme streams).
- `select_all_streams()` indique à la source d'inclure tous les streams disponibles lors de la lecture.
- Alternative : `source.select_streams(["stream_name"])` pour sélectionner seulement certains streams (utile pour filtrer).

***

### 4. Lecture des données

```python
result = source.read()
```

**Concept : Exécution du connecteur et lecture**

- Lance l'extraction des données depuis la source.
- La source parcourt chaque stream sélectionné et produit un **flux de records** (enregistrements individuels).
- `result` est un objet contenant les données sous forme structurée avec métadonnées (schéma, état, etc.).
- Cette opération peut être longue sur des sources de grande taille.

---

### 5. Itération et affichage

```python
for name, records in result.streams.items():
    print(f"Stream {name}: {len(list(records))} records")
```

**Concept : Énumération des streams et comptage des records**

- `result.streams` : dictionnaire où chaque clé est le nom du stream, la valeur est un itérateur de records.
- `items()` : itère sur les paires (nom du stream, données).
- `list(records)` : convertit l'itérateur en liste pour compter le nombre d'enregistrements avec `len()`.
- Affiche pour chaque stream le nombre de records traités.

***

## Flux global et concepts architecturaux

```
┌─────────────────────────────────────────────────────────────┐
│                    Source Airbyte                           │
├─────────────────────────────────────────────────────────────┤
│  1. get_source() : Charge le connecteur "source-faker"      │
│  2. check()      : Valide la configuration                  │
│  3. select_all_streams() : Sélectionne les streams à lire  │
│  4. read()       : Exécute l'extraction des données         │
│  5. Itération   : Traite les records par stream            │
└─────────────────────────────────────────────────────────────┘
```

***

## Concepts clés en synthèse

| Concept | Définition | Exemple dans le code |
|---------|-----------|----------------------|
| **Source** | Système d'origine de données | `ab.get_source("source-faker")` |
| **Connecteur** | Implémentation logicielle pour accéder à une source | `source-faker` |
| **Configuration** | Paramètres d'accès à la source | `config={"count": 5_000}` |
| **Stream** | Flux logique de données (table, collection, etc.) | Chaque itération de `result.streams` |
| **Record** | Enregistrement individuel (ligne, document, etc.) | Chaque élément de `records` |
| **Check** | Validation de la connectivité et configuration | `source.check()` |
| **Lecture** | Extraction et récupération des données | `source.read()` |

***

## Résumé opérationnel

Ce code teste la **pipeline d'extraction de données** d'Airbyte en :

1. Initialisant une source faker (données fictives pour tests).
2. Validant la configuration.
3. Sélectionnant tous les streams disponibles.
4. Exécutant l'extraction et comptant les enregistrements par stream.

En contexte réel (production), le même pattern s'appliquerait à une vraie source (PostgreSQL, Salesforce, API, etc.) pour valider et lire ses données avant chargement dans une destination.

# Installation
Récupère l'image
    curl -LsfS https://get.airbyte.com | bash -
Vérifie l'installation
    abctl version
Se placer dans le root du projet
    abctl local install
Saisir Email & organisation sur localhost:8000
Générer la config :

abctl local credentials
  INFO    Using Kubernetes provider:
            Provider: kind
            Kubeconfig: /home/yves/.airbyte/abctl/abctl.kubeconfig
            Context: kind-airbyte-abctl
 SUCCESS  Retrieving your credentials from 'airbyte-auth-secrets'
  INFO    Credentials:
            Email: [not set]
            Password: ACPwP2GvX2hNxEzh2dGUecDCbeGfhbbf
            Client-Id: e4093f1f-51a1-4163-92e3-bbf8658975c3
            Client-Secret: qOnTpr93LzVDRySwPfOmiwx3HpE3ZCVg
            
# Configuration Airbyte - GreenAndCoop

## Installation
Airbyte est installé avec `abctl` au niveau utilisateur.

## Emplacements importants

### Données Airbyte (utilisateur)
- Config : `~/.airbyte/abctl/`
- Base de données : `~/.airbyte/db/`
- Workspace : `~/.airbyte/workspace/`
- Logs : `~/.airbyte/logs/`

### Code Projet (git)
- Racine : `~/greencoop-forecast-2.0/`
- Documentation Airbyte : `~/greencoop-forecast-2.0/airbyte/`

## Accès
- URL : http://localhost:8000
- API : http://localhost:8001

## Commandes de gestion

To stop running all containers, but keep your data:

abctl local uninstall


# Problème de droits avec la base Postgres
apt install kubectl en local
Le contexte `kind-airbyte-abctl` n'existe pas, ce qui signifie que le cluster Kubernetes créé par `abctl` a peut-être un nom différent ou que kubectl n'est pas configuré pour y accéder.

### 1. Lister les contextes kubectl disponibles

Pour voir tous les contextes configurés :

```bash
kubectl config get-contexts
```

Cherche un contexte qui contient `kind`, `airbyte` ou un nom similaire.

***

### 2. Si aucun contexte kind n'apparaît

Cela peut signifier que `abctl` a créé le cluster kind mais que kubectl n'est pas encore configuré. Tu peux :

1. **Vérifier que kind est installé** :
   ```bash
   kind version
   ```

2. **Lister les clusters kind** :
   ```bash
   kind get clusters
   ```
   Tu devrais voir quelque chose comme `airbyte-abctl` ou un nom similaire.

3. **Configurer kubectl pour accéder au cluster** :
   ```bash
   kind export kubeconfig --name <nom_du_cluster>
   ```
   Par exemple, si le cluster s'appelle `airbyte-abctl` :
   ```bash
   kind export kubeconfig --name airbyte-abctl
   ```

4. **Vérifier les contextes à nouveau** :
   ```bash
   kubectl config get-contexts
   ```

***

### 3. Une fois le contexte trouvé ou créé

Switch vers le contexte approprié :

```bash
kubectl config use-context kind-<nom_du_cluster>
```

Puis liste les pods :

```bash
kubectl get pods -A
```

***
kubectl exec -n airbyte-abctl airbyte-db-0 -- id
uid=70(postgres) gid=70(postgres) groups=70(postgres)
Parfait, je vais te faire un **guide clair et complet pour mettre en place le connecteur custom “Excel avec sheet_name” dans Airbyte local/Kubernetes**, étape par étape, sans ambiguïté.
On va reprendre **depuis zéro** et structurer correctement l’ensemble.

---

# 🔹 1️⃣ Objectif

Créer un **connecteur Airbyte custom** qui :

1. Lit un fichier Excel (HTTP ou S3 public) contenant plusieurs feuilles.
2. Ajoute une colonne `sheet_name` indiquant le nom de la feuille d’origine.
3. Renvoie **un seul stream**, compatible ELT, vers n’importe quelle destination (S3, BigQuery, Snowflake…).

---

# 🔹 2️⃣ Structure des fichiers

On crée un dossier par exemple `custom_excel_with_sheet/` avec :

```
custom_excel_with_sheet/
│
├─ Dockerfile
├─ requirements.txt
├─ source.py
└─ manifest.yaml
```

---

# 🔹 3️⃣ Fichiers détaillés

### 3a. `requirements.txt`

```txt
airbyte-cdk
pandas
openpyxl
requests
```

---

### 3b. `source.py` (connecteur Python)

C’est le coeur fonctionnel. Airbyte CDK lira ce script.

```python
import io
import requests
import pandas as pd
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, Type


class SourceExcelWithSheet(AbstractSource):

    def check_connection(self, logger, config):
        try:
            r = requests.get(config["url"])
            r.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config):
        return [ExcelStream(config)]


class ExcelStream:
    def __init__(self, config):
        self.url = config["url"]
        self.name = "excel_with_sheet"

    def read_records(self, **kwargs):
        # Télécharger le fichier
        r = requests.get(self.url)
        r.raise_for_status()
        excel_bytes = io.BytesIO(r.content)

        # Lire toutes les feuilles
        xls = pd.ExcelFile(excel_bytes)
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)

            # Ajouter une colonne sheet_name
            df["sheet_name"] = sheet

            for _, row in df.iterrows():
                yield row.to_dict()
```

---

### 3c. `Dockerfile`

```dockerfile
FROM airbyte/python-connector-base:1.0.0

WORKDIR /airbyte

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY source.py .

ENTRYPOINT ["python", "/airbyte/source.py"]
```

---

### 3d. `manifest.yaml`

Déclare le connecteur pour Airbyte :

```yaml
version: "0.1.0"

spec:
  connectionSpecification:
    type: object
    required:
      - url
    properties:
      url:
        type: string
        description: "URL du fichier Excel à lire (HTTP ou S3 public)"
  documentationUrl: "https://airbyte.io"
```

⚠️ Ici on garde un manifest minimal car tout le code de lecture est dans `source.py`.

---

# 🔹 4️⃣ Construction de l’image Docker

Dans ton terminal, depuis `custom_excel_with_sheet/` :

```bash
docker build -t custom-excel-with-sheet:latest .
```

---

# 🔹 5️⃣ Rendre l’image accessible à Kubernetes (KinD)

### Option 1 : injecter l’image dans le cluster KinD

```bash
kind load docker-image custom-excel-with-sheet:latest --name airbyte
```

Remplace `airbyte` par le nom de ton cluster KinD utilisé par `abctl`. (kind get clusters)

### Option 2 : utiliser un registre local (optionnel)

```bash
docker tag custom-excel-with-sheet:latest localhost:5000/custom-excel-with-sheet:latest
docker push localhost:5000/custom-excel-with-sheet:latest
```

---

# 🔹 6️⃣ Ajouter le connecteur dans Airbyte

1. Ouvre l’UI Airbyte (`http://localhost:8000`)
2. **Sources → Add source → Custom connector → Use a custom Docker image**
3. Nom : `Excel with sheet_name`
4. Image : `custom-excel-with-sheet:latest` (ou le registre si utilisé)
5. Paramètre `url` : `https://...ton-fichier.xlsx...`

---

# 🔹 7️⃣ Créer la connexion vers la destination

* Destination : S3, BigQuery, Snowflake…
* Laisse Airbyte créer un **stream unique** : `excel_with_sheet`
* Chaque ligne contiendra maintenant `sheet_name` → **nom de l’onglet original**

Exemple de sortie :

```
Time        | Temperature | Wind | Humidity | sheet_name
2024-10-01  | 14.3        | NW   | 72%      | 011024
2024-10-01  | 16.1        | W    | 70%      | 011024
2024-10-02  | ...         | ...  | ...      | 021024
...
```

---

# 🔹 8️⃣ Vérifications

1. Lancer un **Sync**
2. Vérifier que les lignes de toutes les feuilles sont présentes
3. Vérifier que `sheet_name` correspond bien aux onglets
4. Vérifier dans la destination (S3 / warehouse) que la colonne est bien incluse

---

# 🔹 9️⃣ Points clés de compatibilité

| Élément                  | Compatible ? | Commentaire                                         |
| ------------------------ | ------------ | --------------------------------------------------- |
| Airbyte Local (`abctl`)  | ✔️           | Fonctionne sans modification                        |
| Kubernetes (KinD, k3s)   | ✔️           | Pods éphémères pour le connecteur                   |
| Docker local             | ✔️           | Utilisé pour build de l’image                       |
| Connecteur Python custom | ✔️           | Airbyte lance l’image dans un pod                   |
| ELT-friendly (1 stream)  | ✔️           | Oui, toutes les feuilles concaténées + `sheet_name` |

---

# 🔹 ✅ Résultat final

* 1 stream → compatible ELT
* Toutes les feuilles du fichier Excel sont lues
* La colonne `sheet_name` permet d’identifier la feuille d’origine
* Compatible avec ton Airbyte local + Kubernetes

---

Si tu veux, je peux te fournir **une version complète clé-en-main avec JSON Schema** déjà intégré pour le stream, ce qui permet d’avoir tous les types de colonnes détectés automatiquement dans Airbyte et les destinations.

Veux-tu que je fasse ça ?


