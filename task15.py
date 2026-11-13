
import time

from redis_queue import RedisTaskQueue
from pipeline import run_pipeline
from metrics import (
    record_success,
    record_failure,
    update_queue_size,
    update_memory_usage
)


def run_tests():

    print("\n========================================")
    print(" ENTERPRISE DATA INGESTION GATEWAY")
    print("          FINAL TEST SUITE")
    print("========================================\n")

    passed = 0
    total = 7

    # Dedicated queue prevents background workers
    # from consuming integration test tasks.
    queue = RedisTaskQueue(
        queue_name="task15:integration_test"
    )

    try:

        # Test 1: Redis connection
        if queue.health_check():
            print("Test 1: Redis connection - PASS")
            passed += 1

        # Test 2: Task submission
        queue.clear_queue()

        payload = {
            "dataset": "final_customer_data",
            "records": [
                {
                    "customer_id": 401,
                    "name": " Alice ",
                    "amount": 3000
                },
                {
                    "customer_id": 402,
                    "name": " Bob ",
                    "amount": 4000
                }
            ]
        }

        task_id = queue.enqueue(payload)

        if task_id:
            print("Test 2: Task submission - PASS")
            passed += 1

        # Test 3: Task retrieval
        task = queue.dequeue(timeout=5)

        if task is not None and task.get("task_id") == task_id:
            print("Test 3: Task retrieval - PASS")
            passed += 1
        else:
            raise RuntimeError(
                "Task retrieval failed. "
                "The submitted task was not found."
            )

        # Test 4: Pipeline execution
        start = time.perf_counter()

        result = run_pipeline(task["payload"])

        duration = time.perf_counter() - start

        if result is not None:
            print("Test 4: Pipeline execution - PASS")
            passed += 1

            record_success(duration)

        else:
            raise RuntimeError("Pipeline returned no result.")

        # Test 5: Queue monitoring
        current_queue_size = queue.queue_size()

        update_queue_size(current_queue_size)

        if current_queue_size == 0:
            print("Test 5: Queue monitoring - PASS")
            passed += 1

        # Test 6: Memory monitoring
        memory = update_memory_usage()

        if memory > 0:
            print("Test 6: Memory monitoring - PASS")
            print(f"         Memory usage: {memory:.2f} MB")
            passed += 1

        # Test 7: Task metrics
        if duration >= 0:
            print("Test 7: Task metrics recording - PASS")
            passed += 1

        print("\n========================================")
        print(f" FINAL RESULT: {passed}/{total} TESTS PASSED")
        print("========================================")

        if passed == total:
            print("\nTASK 15 FINAL TESTING SUCCESSFUL!")

        else:
            print("\nSOME TESTS DID NOT PASS.")

    except Exception as error:

        record_failure()

        print("\nTEST FAILED:", error)

        print(f"Tests passed before failure: {passed}/{total}")

    finally:

        queue.clear_queue()
        queue.close()

        print("\nIntegration test queue cleaned up.")


if __name__ == "__main__":
    run_tests()
