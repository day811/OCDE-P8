# src/preprocessing/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Copier requirements et installer dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script
COPY unified_data_pipeline.py .


# Exécuter le script au démarrage
ENTRYPOINT ["python", "unified_data_pipeline.py"]
