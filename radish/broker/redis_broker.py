import json
import time
from typing import Optional, Tuple, List

from redis import Redis

from radish.models import TaskMessage


class RedisBroker:
    """
    Redis-backed broker for task delivery and crash recovery.

    Ready tasks are stored in:
        radish:queue:{queue_name}

    Reserved but not yet acknowledged tasks are stored in:
        radish:processing:{queue_name}
    """

    def __init__(self, redis_url: str):
        """
        Create a Redis client from a connection URL.

        Example:
            redis://localhost:6379/0
        """
        self.redis = Redis.from_url(redis_url, decode_responses=True)

    def _queue_key(self, queue: str) -> str:
        """
        Return the Redis key for the ready queue.
        """
        return f"radish:queue:{queue}"

    def _processing_key(self, queue: str) -> str:
        """
        Return the Redis key for the processing queue.
        """
        return f"radish:processing:{queue}"

    def enqueue(self, message: TaskMessage) -> None:
        """
        Push a new task into the ready queue.

        The task is serialized to JSON before being stored in Redis.
        """
        key = self._queue_key(message["queue"])
        self.redis.lpush(key, json.dumps(message))

    def reserve(self, queue: str, timeout: int = 5) -> Optional[Tuple[str, TaskMessage]]:
        """
        Atomically move one task from the ready queue to the processing queue.

        This method does not remove the task permanently. It only marks the task
        as reserved by moving it to the processing queue. The task will stay there
        until the worker acknowledges it.

        Returns:
            None:
                If no task is available within the timeout.

            (updated_raw_message, parsed_message):
                The JSON string stored in the processing queue, and the parsed task.
        """
        ready_key = self._queue_key(queue)
        processing_key = self._processing_key(queue)

        raw_message = self.redis.brpoplpush(ready_key, processing_key, timeout=timeout)
        if raw_message is None:
            return None

        message: TaskMessage = json.loads(raw_message)
        message["reserved_at"] = time.time()
        #update reserved time
        updated_raw_message = json.dumps(message)

        self.redis.lrem(processing_key, 1, raw_message)
        self.redis.lpush(processing_key, updated_raw_message)

        return updated_raw_message, message

    def ack(self, queue: str, raw_message: str) -> None:
        """
        Acknowledge that a reserved task has been fully handled.

        This removes the task from the processing queue.
        """
        processing_key = self._processing_key(queue)
        self.redis.lrem(processing_key, 1, raw_message)

    def requeue(self, queue: str, raw_message: str, updated_message: TaskMessage) -> None:
        """
        Requeue a reserved task back to the ready queue.

        This is used for retry:
            1. Remove the old reserved message from the processing queue
            2. Push the updated task message back to the ready queue
        """
        processing_key = self._processing_key(queue)
        ready_key = self._queue_key(queue)

        self.redis.lrem(processing_key, 1, raw_message)
        self.redis.lpush(ready_key, json.dumps(updated_message))

    def requeue_stale_tasks(self, queue: str, visibility_timeout: int) -> int:
        """
        Move stale reserved tasks from the processing queue back to the ready queue.

        A task is stale if:
            reserved_at is not None
            and current_time - reserved_at >= visibility_timeout

        This provides crash recovery:
            if a worker reserves a task and crashes before ack,
            the task can become visible again and be retried later.

        Returns:
            The number of tasks moved back to the ready queue.
        """
        processing_key = self._processing_key(queue)
        ready_key = self._queue_key(queue)

        current_time = time.time()
        moved_count = 0

        raw_messages: List[str] = self.redis.lrange(processing_key, 0, -1)

        for raw_message in raw_messages:
            message: TaskMessage = json.loads(raw_message)
            reserved_at = message.get("reserved_at")

            if reserved_at is None:
                continue

            if current_time - reserved_at >= visibility_timeout:
                message["reserved_at"] = None
                updated_raw_message = json.dumps(message)

                removed = self.redis.lrem(processing_key, 1, raw_message)
                if removed:
                    self.redis.lpush(ready_key, updated_raw_message)
                    moved_count += 1

        return moved_count