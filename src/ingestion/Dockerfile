# src/ingestion/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Copier le script
WORKDIR /app

# copy sources to /app
COPY src/ingestion .
COPY config.yaml .

# Installer dépendances
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "mongo_loader.py"]
