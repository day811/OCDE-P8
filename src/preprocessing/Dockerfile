# src/preprocessing/Dockerfile

FROM python:3.11-slim

# Copier le script
WORKDIR /app

# copy sources to /app
COPY src/preprocessing .
COPY config.yaml .

# Installer dépendances
RUN pip install --no-cache-dir -r requirements.txt


# Exécuter le script au démarrage
ENTRYPOINT ["python", "unified_data_pipeline.py"]
