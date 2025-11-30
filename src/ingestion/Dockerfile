# src/ingestion/Dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mongo_loader.py .

ENTRYPOINT ["python", "mongo_loader.py"]
