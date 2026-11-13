
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from redis_queue import RedisTaskQueue
from scheduler import IngestionScheduler
from metrics import (
    update_queue_size,
    update_memory_usage,
    get_metrics,
    get_metrics_content_type
)


# -----------------------------------------
# APPLICATION CONFIGURATION
# -----------------------------------------

scheduler = None
queue = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler, queue

    queue = RedisTaskQueue()

    try:
        if not queue.health_check():
            raise RuntimeError("Redis is unavailable.")

        scheduler = IngestionScheduler(num_workers=2)

        # Start workers and scheduler
        scheduler.start()

        print("Task 15 API started successfully.")

        yield

    finally:
        if scheduler:
            scheduler.close()

        if queue:
            queue.close()

        print("Task 15 API stopped.")


app = FastAPI(
    title="Enterprise Data Ingestion Gateway",
    description=(
        "API for scheduled multiprocessing data ingestion "
        "with Redis and Prometheus monitoring."
    ),
    version="1.0.0",
    lifespan=lifespan
)


# -----------------------------------------
# REQUEST MODEL
# -----------------------------------------

class IngestionRequest(BaseModel):
    dataset: str = Field(
        ...,
        min_length=1,
        description="Dataset name"
    )

    records: list[dict] = Field(
        ...,
        min_length=1,
        description="Records to ingest"
    )


# -----------------------------------------
# ROOT ENDPOINT
# -----------------------------------------

@app.get("/")
def root():

    return {
        "message": "Enterprise Data Ingestion Gateway",
        "status": "running",
        "version": "1.0.0"
    }


# -----------------------------------------
# HEALTH ENDPOINT
# -----------------------------------------

@app.get("/health")
def health():

    try:

        redis_status = queue.health_check()

        return {
            "api": "healthy",
            "redis": "connected" if redis_status else "disconnected"
        }

    except Exception as error:

        raise HTTPException(
            status_code=503,
            detail=f"Health check failed: {error}"
        )


# -----------------------------------------
# INGESTION ENDPOINT
# -----------------------------------------

@app.post("/ingest", status_code=202)
def ingest_data(request: IngestionRequest):

    try:

        payload = request.model_dump()

        task_id = queue.enqueue(payload)

        update_queue_size(queue.queue_size())

        return {
            "message": "Data submitted successfully.",
            "task_id": task_id,
            "status": "pending"
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=f"Data submission failed: {error}"
        )


# -----------------------------------------
# QUEUE STATUS ENDPOINT
# -----------------------------------------

@app.get("/queue")
def queue_status():

    try:

        size = queue.queue_size()

        update_queue_size(size)

        return {
            "pending_tasks": size
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(error)
        )


# -----------------------------------------
# SCHEDULED JOBS ENDPOINT
# -----------------------------------------

@app.get("/jobs")
def get_scheduled_jobs():

    return {
        "jobs": scheduler.get_jobs()
    }


# -----------------------------------------
# PROMETHEUS METRICS ENDPOINT
# -----------------------------------------

@app.get("/metrics")
def prometheus_metrics():

    update_queue_size(queue.queue_size())
    update_memory_usage()

    return Response(
        content=get_metrics(),
        media_type=get_metrics_content_type()
    )


# -----------------------------------------
# API TESTING
# -----------------------------------------

@app.get("/test")
def test_api():

    return {
        "message": "API endpoint test successful.",
        "available_endpoints": [
            "/",
            "/health",
            "/ingest",
            "/queue",
            "/jobs",
            "/metrics",
            "/docs"
        ]
    }
