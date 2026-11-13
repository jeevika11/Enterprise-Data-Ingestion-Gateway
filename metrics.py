
import os
import psutil

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    REGISTRY,
    multiprocess,
    generate_latest,
    CONTENT_TYPE_LATEST
)


# -----------------------------------------
# PROMETHEUS REGISTRY CONFIGURATION
# -----------------------------------------

PROMETHEUS_MULTIPROC_DIR = os.environ.get(
    "PROMETHEUS_MULTIPROC_DIR"
)

if PROMETHEUS_MULTIPROC_DIR:

    # Collect metrics from multiple processes
    scrape_registry = CollectorRegistry()

    multiprocess.MultiProcessCollector(
        scrape_registry
    )

else:

    # Normal single-process monitoring
    scrape_registry = REGISTRY


# -----------------------------------------
# METRIC DEFINITIONS
# -----------------------------------------

TASKS_PROCESSED = Counter(
    "task15_tasks_processed_total",
    "Total number of successfully processed tasks"
)

TASKS_FAILED = Counter(
    "task15_tasks_failed_total",
    "Total number of failed tasks"
)

TASK_DURATION = Histogram(
    "task15_task_duration_seconds",
    "Time taken to process a task"
)

QUEUE_SIZE = Gauge(
    "task15_queue_size",
    "Number of tasks waiting in Redis queue",
    multiprocess_mode="livesum"
)

MEMORY_USAGE = Gauge(
    "task15_memory_usage_mb",
    "Process memory usage in MB",
    multiprocess_mode="max"
)


# -----------------------------------------
# SUCCESS METRICS
# -----------------------------------------

def record_success(duration):

    TASKS_PROCESSED.inc()

    TASK_DURATION.observe(duration)


# -----------------------------------------
# FAILURE METRICS
# -----------------------------------------

def record_failure(duration=None):

    TASKS_FAILED.inc()

    if duration is not None:
        TASK_DURATION.observe(duration)


# -----------------------------------------
# QUEUE METRICS
# -----------------------------------------

def update_queue_size(size):

    QUEUE_SIZE.set(size)


# -----------------------------------------
# MEMORY METRICS
# -----------------------------------------

def update_memory_usage():

    process = psutil.Process(os.getpid())

    memory_mb = (
        process.memory_info().rss / (1024 * 1024)
    )

    MEMORY_USAGE.set(memory_mb)

    return memory_mb


# -----------------------------------------
# EXPORT PROMETHEUS METRICS
# -----------------------------------------

def get_metrics():

    return generate_latest(scrape_registry)


def get_metrics_content_type():

    return CONTENT_TYPE_LATEST


# -----------------------------------------
# TESTING
# -----------------------------------------

if __name__ == "__main__":

    print("\n===================================")
    print("   PROMETHEUS MULTIPROCESS TEST")
    print("===================================\n")

    record_success(0.25)
    record_success(0.50)

    record_failure(0.15)

    update_queue_size(5)

    memory = update_memory_usage()

    print("Processed tasks:", TASKS_PROCESSED._value.get())
    print("Failed tasks:", TASKS_FAILED._value.get())
    print("Queue size:", QUEUE_SIZE._value.get())

    print(f"Memory usage: {memory:.2f} MB")

    print("\nPrometheus Metrics:\n")

    print(get_metrics().decode("utf-8"))

    print("===================================")
    print("       TEST COMPLETED")
    print("===================================")
