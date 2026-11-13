
import time
import threading
import multiprocessing

from redis_queue import RedisTaskQueue
from worker import WorkerManager


class IngestionScheduler:
    """
    Schedule recurring data ingestion jobs
    and manage multiprocessing workers.
    """

    def __init__(self, num_workers=2):

        self.queue = RedisTaskQueue()
        self.worker_manager = WorkerManager(num_workers)

        self.jobs = {}
        self.running = False
        self.scheduler_thread = None

        self.lock = threading.Lock()

    # -------------------------------------
    # ADD A SCHEDULED JOB
    # -------------------------------------

    def add_job(
        self,
        job_id,
        payload,
        interval_seconds
    ):

        if not job_id:
            raise ValueError("Job ID cannot be empty.")

        if not isinstance(payload, dict) or not payload:
            raise ValueError("Payload must be a non-empty dictionary.")

        if interval_seconds <= 0:
            raise ValueError("Interval must be greater than zero.")

        with self.lock:

            if job_id in self.jobs:
                raise ValueError("Job ID already exists.")

            self.jobs[job_id] = {
                "payload": payload,
                "interval": interval_seconds,
                "next_run": time.monotonic() + interval_seconds,
                "enabled": True,
                "runs": 0
            }

        print(f"Job '{job_id}' scheduled successfully.")

    # -------------------------------------
    # REMOVE A JOB
    # -------------------------------------

    def remove_job(self, job_id):

        with self.lock:

            if job_id not in self.jobs:
                return False

            del self.jobs[job_id]

        print(f"Job '{job_id}' removed.")
        return True

    # -------------------------------------
    # ENABLE / DISABLE A JOB
    # -------------------------------------

    def set_job_enabled(self, job_id, enabled):

        with self.lock:

            if job_id not in self.jobs:
                return False

            self.jobs[job_id]["enabled"] = bool(enabled)

            if enabled:
                self.jobs[job_id]["next_run"] = (
                    time.monotonic()
                    + self.jobs[job_id]["interval"]
                )

        return True

    # -------------------------------------
    # EXECUTE DUE JOBS
    # -------------------------------------

    def run_pending_jobs(self):

        current_time = time.monotonic()
        due_jobs = []

        with self.lock:

            for job_id, job in self.jobs.items():

                if (
                    job["enabled"]
                    and current_time >= job["next_run"]
                ):

                    due_jobs.append(
                        (job_id, job["payload"])
                    )

                    job["runs"] += 1

                    job["next_run"] = (
                        current_time + job["interval"]
                    )

        for job_id, payload in due_jobs:

            try:

                task_id = self.queue.enqueue(payload)

                print(
                    f"[SCHEDULER] Job '{job_id}' "
                    f"submitted. Task ID: {task_id}",
                    flush=True
                )

            except Exception as error:

                print(
                    f"[SCHEDULER] Job '{job_id}' failed: {error}",
                    flush=True
                )

    # -------------------------------------
    # SCHEDULER LOOP
    # -------------------------------------

    def _scheduler_loop(self):

        print("[SCHEDULER] Started.")

        while self.running:

            try:
                self.run_pending_jobs()

            except Exception as error:
                print("[SCHEDULER] Error:", error)

            time.sleep(1)

        print("[SCHEDULER] Stopped.")

    # -------------------------------------
    # START SCHEDULER AND WORKERS
    # -------------------------------------

    def start(self):

        if self.running:
            print("Scheduler is already running.")
            return

        if not self.queue.health_check():
            raise ConnectionError("Redis is unavailable.")

        self.running = True

        self.worker_manager.start()

        self.scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True
        )

        self.scheduler_thread.start()

        print("[SCHEDULER] Service started successfully.")

    # -------------------------------------
    # STOP SCHEDULER AND WORKERS
    # -------------------------------------

    def stop(self):

        if not self.running:
            return

        self.running = False

        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

        self.worker_manager.stop()

        print("[SCHEDULER] Service stopped.")

    # -------------------------------------
    # GET JOB INFORMATION
    # -------------------------------------

    def get_jobs(self):

        with self.lock:
            return {
                job_id: {
                    "interval": job["interval"],
                    "enabled": job["enabled"],
                    "runs": job["runs"]
                }
                for job_id, job in self.jobs.items()
            }

    def close(self):

        self.stop()
        self.queue.close()


# -----------------------------------------
# SCHEDULER TESTING
# -----------------------------------------

def test_scheduler():

    print("\n===================================")
    print("       SCHEDULER TESTING")
    print("===================================\n")

    scheduler = IngestionScheduler(num_workers=1)

    try:

        scheduler.queue.clear_queue()

        sample_payload = {
            "dataset": "scheduled_customer_data",
            "records": [
                {
                    "customer_id": 201,
                    "name": " Alice ",
                    "amount": 4000
                },
                {
                    "customer_id": 202,
                    "name": " Bob ",
                    "amount": 6000
                }
            ]
        }

        # Test 1: Add job
        scheduler.add_job(
            "customer_ingestion",
            sample_payload,
            interval_seconds=2
        )

        if "customer_ingestion" in scheduler.get_jobs():
            print("Test 1: Add scheduled job - PASS")
        else:
            print("Test 1: Add scheduled job - FAIL")
            return

        # Test 2: Job information
        job_info = scheduler.get_jobs()

        if job_info["customer_ingestion"]["interval"] == 2:
            print("Test 2: Job configuration - PASS")
        else:
            print("Test 2: Job configuration - FAIL")

        # Test 3: Disable job
        result = scheduler.set_job_enabled(
            "customer_ingestion",
            False
        )

        if (
            result
            and not scheduler.get_jobs()["customer_ingestion"]["enabled"]
        ):
            print("Test 3: Disable job - PASS")
        else:
            print("Test 3: Disable job - FAIL")

        # Test 4: Enable job
        result = scheduler.set_job_enabled(
            "customer_ingestion",
            True
        )

        if (
            result
            and scheduler.get_jobs()["customer_ingestion"]["enabled"]
        ):
            print("Test 4: Enable job - PASS")
        else:
            print("Test 4: Enable job - FAIL")

        # Test 5: Remove job
        result = scheduler.remove_job("customer_ingestion")

        if (
            result
            and "customer_ingestion" not in scheduler.get_jobs()
        ):
            print("Test 5: Remove job - PASS")
        else:
            print("Test 5: Remove job - FAIL")

        print("\n===================================")
        print("       SCHEDULER TEST COMPLETED")
        print("===================================")

    finally:

        scheduler.close()
        scheduler.queue.clear_queue()


if __name__ == "__main__":

    multiprocessing.freeze_support()
    test_scheduler()
