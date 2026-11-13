
import time
import multiprocessing
import traceback

from redis_queue import RedisTaskQueue
from pipeline import run_pipeline
from metrics import (
    record_success,
    record_failure,
    update_queue_size,
    update_memory_usage
)


def process_task(task):

    start_time = time.perf_counter()

    try:
        result = run_pipeline(task["payload"])

        duration = time.perf_counter() - start_time

        record_success(duration)

        print(
            f"[SUCCESS] Task {task['task_id']} "
            f"completed in {duration:.4f} seconds",
            flush=True
        )

        return result

    except Exception as error:

        duration = time.perf_counter() - start_time

        record_failure(duration)

        print(
            f"[FAILED] Task {task.get('task_id', 'UNKNOWN')}: {error}",
            flush=True
        )

        traceback.print_exc()

        return None


def worker_process(worker_id, stop_event):

    queue = RedisTaskQueue()

    print(f"[WORKER {worker_id}] Started", flush=True)

    try:

        while not stop_event.is_set():

            try:

                task = queue.dequeue(timeout=2)

                update_queue_size(queue.queue_size())
                update_memory_usage()

                if task is None:
                    continue

                print(
                    f"[WORKER {worker_id}] "
                    f"Received task {task['task_id']}",
                    flush=True
                )

                process_task(task)

                update_queue_size(queue.queue_size())

            except Exception as error:

                print(
                    f"[WORKER {worker_id}] Error: {error}",
                    flush=True
                )

                time.sleep(2)

    finally:

        queue.close()

        print(
            f"[WORKER {worker_id}] Stopped",
            flush=True
        )


class WorkerManager:

    def __init__(self, num_workers=2):

        if num_workers < 1:
            raise ValueError("At least one worker is required.")

        self.num_workers = num_workers
        self.stop_event = multiprocessing.Event()
        self.processes = []

    def start(self):

        if self.processes:
            raise RuntimeError("Workers are already running.")

        self.stop_event.clear()

        for worker_id in range(1, self.num_workers + 1):

            process = multiprocessing.Process(
                target=worker_process,
                args=(worker_id, self.stop_event),
                name=f"Task15-Worker-{worker_id}"
            )

            process.start()
            self.processes.append(process)

        print(
            f"{self.num_workers} workers started successfully.",
            flush=True
        )

    def stop(self):

        self.stop_event.set()

        for process in self.processes:
            process.join(timeout=5)

        for process in self.processes:

            if process.is_alive():
                process.terminate()
                process.join()

        self.processes.clear()

        print("All workers stopped.", flush=True)


def test_worker():

    print("\n===================================")
    print("       WORKER TESTING")
    print("===================================\n")

    queue = RedisTaskQueue()

    try:

        queue.clear_queue()

        sample_payload = {
            "dataset": "customer_data",
            "records": [
                {
                    "customer_id": 101,
                    "name": " Alice ",
                    "amount": 2500
                },
                {
                    "customer_id": 102,
                    "name": " Bob ",
                    "amount": 3500
                }
            ]
        }

        task_id = queue.enqueue(sample_payload)

        print("Test 1: Task submission - PASS")

        task = queue.dequeue(timeout=2)

        if task and task["task_id"] == task_id:
            print("Test 2: Task retrieval - PASS")
        else:
            print("Test 2: Task retrieval - FAIL")
            return

        result = process_task(task)

        if result is not None:
            print("Test 3: Pipeline execution - PASS")
        else:
            print("Test 3: Pipeline execution - FAIL")

        print("\n===================================")
        print("       WORKER TEST COMPLETED")
        print("===================================")

    finally:

        queue.clear_queue()
        queue.close()


if __name__ == "__main__":

    multiprocessing.freeze_support()
    test_worker()
