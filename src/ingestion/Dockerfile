# src/ingestion/Dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mongo_loader.py .

RUN mkdir -p /app/logs && chmod 777 /app/logs

ENTRYPOINT ["python", "mongo_loader.py"]
