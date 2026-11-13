
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus

RUN mkdir -p /tmp/prometheus

EXPOSE 8000

CMD ["sh", "-c", "rm -f /tmp/prometheus/* && exec uvicorn api:app --host 0.0.0.0 --port 8000"]
