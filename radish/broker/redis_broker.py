import json
import time
from typing import Optional, Tuple, List

from redis import Redis

from radish.broker.broker import Broker  # CHANGED: inherit from Broker
from radish.models import TaskMessage, DLQMessage


class RedisBroker(Broker):  # CHANGED: inherit from Broker
    """
    Redis-backed broker for task delivery and crash recovery.

    Ready tasks are stored in:
        radish:queue:{queue_name}

    Reserved but not yet acknowledged tasks are stored in:
        radish:processing:{queue_name}

    Dead-lettered tasks are stored in:
        radish:dlq:{queue_name}
    """

    def __init__(self, redis_url: str, dlq_max_length: int = 100):
        """
        Create a Redis client from a connection URL.

        Example:
            redis://localhost:6379/0
        """
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.dlq_max_length = dlq_max_length  # CHANGED

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

    def _dlq_key(self, queue: str) -> str:
        """
        Return the Redis key for the dead-letter queue.
        """
        return f"radish:dlq:{queue}"

    def _trim_dlq_if_needed(self, queue: str) -> None:
        """
        Keep only the newest dlq_max_length items in the DLQ list.
        """
        dlq_key = self._dlq_key(queue)
        self.redis.ltrim(dlq_key, 0, self.dlq_max_length - 1)

    def _dlq_to_task_message(self, dlq_message: DLQMessage) -> TaskMessage:
        """
        Convert a DLQ message back into a normal task message for requeue.

        Requeue rules:
            - keep the same task id
            - reset attempt to 1
            - reset reserved_at to None
        """
        return {
            "id": dlq_message["id"],
            "task_name": dlq_message["task_name"],
            "args": dlq_message["args"],
            "kwargs": dlq_message["kwargs"],
            "queue": dlq_message["queue"],
            "created_at": dlq_message["created_at"],
            "reserved_at": None,  # CHANGED: reset for requeue
            "attempt": 1,  # CHANGED: reset attempt for requeue
            "max_retries": dlq_message["max_retries"],
            "retry_delay_ms": dlq_message["retry_delay_ms"],
        }

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
        # update reserved time
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

    def push_to_dlq(self, message: DLQMessage) -> None:
        """
        Push a dead-lettered task into the DLQ.

        The newest message is pushed to the head of the list.
        If the DLQ exceeds dlq_max_length, the oldest messages are trimmed.
        """
        dlq_key = self._dlq_key(message["queue"])
        self.redis.lpush(dlq_key, json.dumps(message))
        self._trim_dlq_if_needed(message["queue"])


    def list_dlq(self, queue: str, limit: int = 100) -> List[DLQMessage]:
        """
        Return the newest DLQ messages for a queue.

        The returned list is ordered from newest to oldest.
        """
        dlq_key = self._dlq_key(queue)
        raw_messages = self.redis.lrange(dlq_key, 0, limit - 1)
        return [json.loads(raw_message) for raw_message in raw_messages]


    def delete_dlq_task(self, queue: str, task_id: str) -> bool:
        """
        Delete one DLQ task by task_id.

        Returns:
            True if one matching message was removed.
            False if no matching task_id was found.
        """
        dlq_key = self._dlq_key(queue)
        raw_messages = self.redis.lrange(dlq_key, 0, -1)

        for raw_message in raw_messages:
            message: DLQMessage = json.loads(raw_message)
            if message["id"] == task_id:
                removed = self.redis.lrem(dlq_key, 1, raw_message)
                return removed > 0

        return False


    def requeue_dlq_task(self, queue: str, task_id: str) -> Optional[TaskMessage]:
        """
        Requeue one DLQ task back to the ready queue.

            1. Find the DLQ message by task_id
            2. Convert it to TaskMessage
            3. Push it back to the ready queue
            4. Remove it from the DLQ
            5. Return the requeued TaskMessage

        Returns:
            The requeued TaskMessage if found and moved successfully.
            None if no matching task_id was found.
        """
        dlq_key = self._dlq_key(queue)
        ready_key = self._queue_key(queue)
        raw_messages = self.redis.lrange(dlq_key, 0, -1)

        for raw_message in raw_messages:
            message: DLQMessage = json.loads(raw_message)
            if message["id"] != task_id:
                continue

            task_message = self._dlq_to_task_message(message)

            pipeline = self.redis.pipeline()
            pipeline.lpush(ready_key, json.dumps(task_message))
            pipeline.lrem(dlq_key, 1, raw_message)
            results = pipeline.execute()

            removed_count = results[1]
            if removed_count > 0:
                return task_message

            return task_message

        return None