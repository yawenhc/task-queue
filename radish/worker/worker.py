# radish/worker/worker.py

"""
This module defines the Worker class.

Responsibilities:
1. Recover stale reserved tasks
2. Reserve new tasks from the broker
3. Execute registered task functions
4. Update task state in the backend
5. Retry failed tasks when allowed
6. Acknowledge final task completion
7. Expose a readable worker identity for logging and CLI usage
"""

import socket
import time
import uuid
from typing import Callable, Dict, Any, Optional

from radish.backend.redis_backend import RedisBackend
from radish.broker.redis_broker import RedisBroker
from radish.models import TaskMessage


class Worker:
    """
    Reliable single-process worker.
    """

    def __init__(
        self,
        broker: RedisBroker,
        backend: RedisBackend,
        registry: Dict[str, Callable[..., Any]],
        queue: str = "default",
        poll_timeout: int = 5,
        visibility_timeout: int = 10,
        worker_id: Optional[str] = None,
    ):
        """
        Initialize a worker instance.

        Args:
            broker:
                The broker used for task delivery and crash recovery.

            backend:
                The backend used for task state and result storage.

            registry:
                Maps task_name to the actual Python callable.

            queue:
                The queue this worker listens to.

            poll_timeout:
                The blocking wait time for reserve().

            visibility_timeout:
                If a reserved task is not acknowledged within this time,
                it is considered stale and can be requeued.

            worker_id:
                Optional explicit worker identifier.
                If not provided, a readable default ID is generated.
        """
        self.broker = broker
        self.backend = backend
        self.registry = registry
        self.queue = queue
        self.poll_timeout = poll_timeout
        self.visibility_timeout = visibility_timeout
        self.running = False
        self.worker_id = worker_id or self._generate_worker_id()

    @staticmethod
    def _generate_worker_id() -> str:
        """
        Generate a readable default worker ID.

        Format:
            worker@hostname-xxxxxxxx
        """
        hostname = socket.gethostname()
        short_uuid = str(uuid.uuid4())[:8]
        return f"worker@{hostname}-{short_uuid}"

    def _log(self, message: str) -> None:
        """
        Print a worker-prefixed log line.
        """
        print(f"[Worker {self.worker_id}] {message}")

    def start(self) -> None:
        """
        Start the worker main loop.

        The worker keeps running until stop() is called
        or a KeyboardInterrupt is received.
        """
        self.running = True
        self._log(
            f"Started. queue={self.queue}, "
            f"poll_timeout={self.poll_timeout}, "
            f"visibility_timeout={self.visibility_timeout}"
        )

        while self.running:
            try:
                self.broker.requeue_stale_tasks(
                    queue=self.queue,
                    visibility_timeout=self.visibility_timeout,
                )

                reserved = self.broker.reserve(
                    queue=self.queue,
                    timeout=self.poll_timeout,
                )

                if reserved is None:
                    continue

                raw_message, message = reserved
                self._handle_task(raw_message, message)

            except KeyboardInterrupt:
                self._log("Stopped by user.")
                self.running = False
            except Exception as exc:
                self._log(f"Worker loop error: {exc}")

    def stop(self) -> None:
        """
        Stop the worker loop gracefully.
        """
        self.running = False
        self._log("Stop signal received.")

    def _handle_task(self, raw_message: str, message: TaskMessage) -> None:
        """
        Execute one reserved task.

        The message is already in the processing queue at this point.
        This method must eventually do one of the following:
            - ack the task after final success
            - ack the task after final failure
            - requeue the task for retry
        """
        task_id = message["id"]
        task_name = message["task_name"]
        attempt = message["attempt"]
        max_retries = message["max_retries"]
        retry_delay_ms = message["retry_delay_ms"]

        self._log(
            f"Picked task. task_id={task_id}, "
            f"task_name={task_name}, attempt={attempt}/{max_retries + 1}"
        )

        func = self.registry.get(task_name)
        if func is None:
            self.backend.set_failure(
                task_id=task_id,
                attempt=attempt,
                max_retries=max_retries,
                error=f"Task '{task_name}' is not registered.",
            )
            self.broker.ack(self.queue, raw_message)
            self._log(
                f"Task failed immediately because it is not registered. "
                f"task_id={task_id}, task_name={task_name}"
            )
            return

        started_at = time.time()

        self.backend.set_started(
            task_id=task_id,
            attempt=attempt,
            max_retries=max_retries,
        )

        try:
            result = func(*message["args"], **message["kwargs"])

            self.backend.set_success(
                task_id=task_id,
                attempt=attempt,
                max_retries=max_retries,
                result_value=result,
                started_at=started_at,
            )

            self.broker.ack(self.queue, raw_message)
            self._log(
                f"Task finished successfully. "
                f"task_id={task_id}, task_name={task_name}"
            )

        except Exception as exc:
            error_message = str(exc)

            if attempt <= max_retries:
                self.backend.set_retrying(
                    task_id=task_id,
                    attempt=attempt,
                    max_retries=max_retries,
                    error=error_message,
                )

                self._log(
                    f"Task failed and will be retried. "
                    f"task_id={task_id}, task_name={task_name}, "
                    f"error={error_message}"
                )

                if retry_delay_ms > 0:
                    time.sleep(retry_delay_ms / 1000)

                updated_message: TaskMessage = {
                    "id": message["id"],
                    "task_name": message["task_name"],
                    "args": message["args"],
                    "kwargs": message["kwargs"],
                    "queue": message["queue"],
                    "created_at": message["created_at"],
                    "reserved_at": None,
                    "attempt": attempt + 1,
                    "max_retries": max_retries,
                    "retry_delay_ms": retry_delay_ms,
                }

                self.broker.requeue(
                    queue=self.queue,
                    raw_message=raw_message,
                    updated_message=updated_message,
                )
            else:
                self.backend.set_failure(
                    task_id=task_id,
                    attempt=attempt,
                    max_retries=max_retries,
                    error=error_message,
                    started_at=started_at,
                )

                self.broker.ack(self.queue, raw_message)
                self._log(
                    f"Task failed permanently. "
                    f"task_id={task_id}, task_name={task_name}, "
                    f"error={error_message}"
                )