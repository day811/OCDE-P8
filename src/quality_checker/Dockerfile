# src/quality_checker/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY quality_checker.py .

RUN mkdir -p /app/logs && chmod 777 /app/logs

# Run quality checker
CMD ["python", "quality_checker.py"]
