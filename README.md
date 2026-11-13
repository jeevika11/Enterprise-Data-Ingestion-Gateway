# Enterprise Data Ingestion Gateway

## Project Overview

The Enterprise Data Ingestion Gateway is a Python-based data ingestion system designed to receive, queue, process, and monitor structured data using scheduled multiprocessing pipelines.

The application uses FastAPI for data submission, Redis for task queuing, multiprocessing workers for parallel task execution, and Prometheus for monitoring system performance.

## Key Features

- REST API for JSON data ingestion using FastAPI.
- Redis-backed task queue.
- Multiprocessing workers for concurrent task processing.
- Sequential data processing pipeline:
  - Data validation
  - Data cleaning
  - Data transformation
- Scheduled job management.
- Prometheus metrics collection.
- Monitoring of processed tasks, failed tasks, execution duration, queue size, and memory usage.
- Docker Compose deployment.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core programming language |
| FastAPI | REST API |
| Redis | Task queue |
| Multiprocessing | Parallel task execution |
| Prometheus | Metrics monitoring |
| Docker | Containerization |
| Docker Compose | Service orchestration |
| psutil | Memory monitoring |

## Project Structure

```text
Task-15/
│
├── api.py
├── redis_queue.py
├── worker.py
├── pipeline.py
├── scheduler.py
├── metrics.py
├── task15.py
│
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── prometheus.yml
└── README.md
```

## System Architecture

1. Client submits JSON data through the FastAPI endpoint.
2. API validates the request and submits a task to Redis.
3. Redis stores pending ingestion tasks.
4. Multiprocessing workers retrieve and execute tasks.
5. The processing pipeline validates, cleans, and transforms the data.
6. Prometheus collects application metrics.
7. Docker Compose manages the API, Redis, and Prometheus services.

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | API information |
| GET | `/health` | Health check |
| POST | `/ingest` | Submit data |
| GET | `/queue` | Queue status |
| GET | `/jobs` | Scheduled jobs |
| GET | `/metrics` | Prometheus metrics |
| GET | `/test` | API test |

## Running the Project

### 1. Build Docker containers

```bash
docker compose build
```

### 2. Start services

```bash
docker compose up -d
```

### 3. Check running containers

```bash
docker compose ps
```

### 4. Open API documentation

```text
http://localhost:8000/docs
```

### 5. Open Prometheus dashboard

```text
http://localhost:9090
```

## Monitoring Metrics

The application exposes the following Prometheus metrics:

- `task15_tasks_processed_total`
- `task15_tasks_failed_total`
- `task15_task_duration_seconds`
- `task15_queue_size`
- `task15_memory_usage_mb`

## Testing

The project was tested through individual module tests and Docker-based integration testing.

Final integration test results:

| Test | Result |
|---|---|
| Redis connection | PASS |
| Task submission | PASS |
| Task retrieval | PASS |
| Pipeline execution | PASS |
| Queue monitoring | PASS |
| Memory monitoring | PASS |
| Task metrics recording | PASS |

**Final Result: 7/7 tests passed.**

Docker deployment was verified with the API, Redis, and Prometheus services running. Prometheus successfully scraped the API, and three successful ingestion tasks were recorded with zero failures during the demonstrated run.

## Conclusion

The Enterprise Data Ingestion Gateway demonstrates a modular data ingestion architecture combining REST APIs, Redis task queuing, multiprocessing, scheduled processing, containerization, and Prometheus-based observability.