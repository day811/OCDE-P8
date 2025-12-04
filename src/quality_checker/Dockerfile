# src/quality_checker/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Copy application
COPY src/quality_checker .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Run quality checker
CMD ["python", "quality_checker.py"]
