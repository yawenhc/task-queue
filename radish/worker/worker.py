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

import logging
import socket
import time
import uuid
from typing import Callable, Dict, Any, Optional

from radish.backend.redis_backend import RedisBackend
from radish.broker.redis_broker import RedisBroker
from radish.models import TaskMessage, DLQMessage, ActiveTaskRecord  # CHANGED
from radish.worker.worker_tracking import WorkerTracking  # CHANGED


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
        logger: Optional[logging.Logger] = None,
        tracking: Optional[WorkerTracking] = None,  # CHANGED
        slot: int = 0,  # CHANGED
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

            logger:
                Optional logger injected by child_runner.
                If not provided, falls back to stdout printing.

            tracking:
                Optional worker active task tracking implementation.

            slot:
                Optional worker slot index used by supervisor and tracking.
        """
        self.broker = broker
        self.backend = backend
        self.registry = registry
        self.queue = queue
        self.poll_timeout = poll_timeout
        self.visibility_timeout = visibility_timeout
        self.running = False
        self.worker_id = worker_id or self._generate_worker_id()
        self.logger = logger
        self.tracking = tracking  # CHANGED
        self.slot = slot  # CHANGED

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

    def _log(self, event: str) -> None:
        """
        Log one worker event.

        Default behavior:
            - use injected logger if available
            - otherwise print to stdout
        """
        if self.logger is not None:
            self.logger.info(event)
        else:
            print(f"[Worker {self.worker_id}] {event}")

    # set active task record
    def _set_active_task(
        self,
        task_id: str,
        task_name: str,
        attempt: int,
        max_retries: int,
        started_at: float,
    ) -> None:
        """
        Store the current active task for this worker slot if tracking is enabled.
        """
        if self.tracking is None:
            return

        record = ActiveTaskRecord()
        record.task_id = task_id
        record.task_name = task_name
        record.queue = self.queue
        record.worker_id = self.worker_id
        record.slot = self.slot
        record.attempt = attempt
        record.max_retries = max_retries
        record.started_at = started_at

        self.tracking.set_active(record)

    # clear active task record
    def _clear_active_task(self) -> None:
        """
        Clear the current active task for this worker slot if tracking is enabled.
        """
        if self.tracking is None:
            return

        self.tracking.clear_active(self.worker_id, self.slot)

    # build DLQ message for normal failures
    def _build_dead_letter_message(
        self,
        message: TaskMessage,
        dead_letter_reason: str,
        failure_type: str,
    ) -> DLQMessage:
        """
        Build a DLQ message from a task message.

        This is used when a task should no longer be retried automatically.
        """
        return {
            "id": message["id"],
            "task_name": message["task_name"],
            "args": message["args"],
            "kwargs": message["kwargs"],
            "queue": message["queue"],
            "created_at": message["created_at"],
            "attempt": message["attempt"],
            "max_retries": message["max_retries"],
            "retry_delay_ms": message["retry_delay_ms"],
            "dead_letter_reason": dead_letter_reason,
            "failure_type": failure_type,
            "dead_lettered_at": time.time(),
        }

    def start(self) -> None:
        """
        Start the worker main loop.

        The worker keeps running until stop() is called
        or a KeyboardInterrupt is received.
        """
        self.running = True
        self._log(
            f"worker_started worker_id={self.worker_id} "
            f"queue={self.queue} poll_timeout={self.poll_timeout} "
            f"visibility_timeout={self.visibility_timeout} slot={self.slot}"  # CHANGED
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
                self._log(f"worker_stopped worker_id={self.worker_id}")
                self.running = False
            except Exception as exc:
                self._log(
                    f"worker_loop_error worker_id={self.worker_id} "
                    f"error_type={type(exc).__name__} error={exc}"
                )

    def stop(self) -> None:
        """
        Stop the worker loop gracefully.
        """
        self.running = False
        self._log(f"worker_stop_requested worker_id={self.worker_id}")

    def _handle_task(self, raw_message: str, message: TaskMessage) -> None:
        """
        Execute one reserved task.

        The message is already in the processing queue at this point.
        This method must eventually do one of the following:
            - ack the task after final success
            - requeue the task for retry
            - dead-letter the task and ack it
        """
        task_id = message["id"]
        task_name = message["task_name"]
        attempt = message["attempt"]
        max_retries = message["max_retries"]
        retry_delay_ms = message["retry_delay_ms"]

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
                f"task_not_registered worker_id={self.worker_id} "
                f"task_id={task_id} task_name={task_name}"
            )
            return

        started_at = time.time()

        self.backend.set_started(
            task_id=task_id,
            attempt=attempt,
            max_retries=max_retries,
        )

        # record active task before function execution
        self._set_active_task(
            task_id=task_id,
            task_name=task_name,
            attempt=attempt,
            max_retries=max_retries,
            started_at=started_at,
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

            # add success log
            self._log(
                f"task_success worker_id={self.worker_id} "
                f"task_id={task_id} task_name={task_name} "
                f"attempt={attempt} max_retries={max_retries}"
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
                    f"task_retrying worker_id={self.worker_id} "
                    f"task_id={task_id} task_name={task_name} "
                    f"attempt={attempt} max_retries={max_retries} "
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
                # dead-letter the task instead of marking plain failure
                dlq_message = self._build_dead_letter_message(
                    message=message,
                    dead_letter_reason="max_tries_exceeded",
                    failure_type="normal_failure",
                )

                self.broker.push_to_dlq(dlq_message)

                self.backend.set_dead_lettered(
                    task_id=task_id,
                    attempt=attempt,
                    max_retries=max_retries,
                    error=error_message,
                    dead_letter_reason="max_tries_exceeded",
                    started_at=started_at,
                )

                self.broker.ack(self.queue, raw_message)
                self._log(
                    f"task_dead_lettered worker_id={self.worker_id} "
                    f"task_id={task_id} task_name={task_name} "
                    f"attempt={attempt} max_retries={max_retries} "
                    f"dead_letter_reason=max_tries_exceeded "
                    f"error={error_message}"
                )
        finally:
            # always clear active task record
            self._clear_active_task()