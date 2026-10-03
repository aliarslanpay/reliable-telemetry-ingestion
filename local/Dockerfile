FROM python:3.12.14-slim-bookworm
WORKDIR /app
COPY ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion /app/ingestion
ENV PYTHONPATH=/app/ingestion
CMD ["python", "-m", "telemetry_ingestion.worker"]
