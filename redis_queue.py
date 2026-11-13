
import json
import uuid
import os
import redis


class RedisTaskQueue:
    """
    Redis-backed task queue for Task 15.
    Supports local and Docker environments.
    """

    def __init__(
        self,
        host=None,
        port=None,
        db=0,
        queue_name="task15:ingestion"
    ):

        self.queue_name = queue_name

        # Read Redis configuration from environment
        host = host or os.getenv("REDIS_HOST", "localhost")
        port = int(port or os.getenv("REDIS_PORT", "6379"))

        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10
        )

    def health_check(self):
        return self.client.ping()

    def enqueue(self, payload):

        if not isinstance(payload, dict) or not payload:
            raise ValueError(
                "Payload must be a non-empty dictionary."
            )

        task = {
            "task_id": str(uuid.uuid4()),
            "payload": payload,
            "status": "pending"
        }

        self.client.rpush(
            self.queue_name,
            json.dumps(task)
        )

        return task["task_id"]

    def dequeue(self, timeout=1):

        if timeout < 0:
            raise ValueError(
                "Timeout cannot be negative."
            )

        result = self.client.blpop(
            self.queue_name,
            timeout=timeout
        )

        if result is None:
            return None

        _, serialized_task = result

        return json.loads(serialized_task)

    def queue_size(self):
        return self.client.llen(self.queue_name)

    def clear_queue(self):
        return self.client.delete(self.queue_name)

    def close(self):
        self.client.close()


# -----------------------------------------
# TESTING
# -----------------------------------------

def test_redis_queue():

    queue = RedisTaskQueue()

    passed = 0
    total = 5

    print("\n===================================")
    print("       REDIS QUEUE TESTING")
    print("===================================\n")

    try:

        queue.clear_queue()

        # Test 1: Redis connection
        if queue.health_check():
            print("Test 1: Redis health check - PASS")
            passed += 1

        # Test 2: Enqueue
        sample_payload = {
            "customer_id": 101,
            "name": "Test Customer",
            "amount": 5000
        }

        task_id = queue.enqueue(sample_payload)

        if task_id:
            print("Test 2: Task enqueue - PASS")
            passed += 1

        # Test 3: Queue size
        if queue.queue_size() == 1:
            print("Test 3: Queue size - PASS")
            passed += 1

        # Test 4: Dequeue
        task = queue.dequeue(timeout=2)

        if task and task["task_id"] == task_id:
            print("Test 4: Task dequeue - PASS")
            passed += 1

        # Test 5: Empty queue
        if queue.queue_size() == 0:
            print("Test 5: Empty queue check - PASS")
            passed += 1

        print("\n===================================")
        print(f"RESULT: {passed}/{total} TESTS PASSED")
        print("===================================\n")

    except Exception as error:
        print("Redis queue test failed:", error)

    finally:
        queue.close()


if __name__ == "__main__":
    test_redis_queue()
