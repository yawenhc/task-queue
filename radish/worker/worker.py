import logging
import socket
import threading
import time
import uuid
import os
from typing import Callable, Dict, Any, Optional

from radish.backend.redis_backend import RedisBackend
from radish.broker.redis_broker import RedisBroker
from radish.models import TaskMessage, DLQMessage, ActiveTaskRecord
from radish.models import WorkerHeartbeatRecord
from radish.worker.worker_tracking import WorkerTracking


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
        tracking: Optional[WorkerTracking] = None,
        slot: int = 0,
    ):
        self.broker = broker
        self.backend = backend
        self.registry = registry
        self.queue = queue
        self.poll_timeout = poll_timeout
        self.visibility_timeout = visibility_timeout
        self.running = False
        self.worker_id = worker_id or self._generate_worker_id()
        self.logger = logger
        self.tracking = tracking
        self.slot = slot

        # heartbeat interval is fixed for now
        self.heartbeat_interval = 2

        # background heartbeat thread state
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop_event = threading.Event()

    @staticmethod
    def _generate_worker_id() -> str:
        hostname = socket.gethostname()
        short_uuid = str(uuid.uuid4())[:8]
        return f"worker@{hostname}-{short_uuid}"

    def _log(self, event: str) -> None:
        if self.logger is not None:
            self.logger.info(event)
        else:
            print(f"[Worker {self.worker_id}] {event}")

    def _send_heartbeat(self) -> None:
        """
        Send one heartbeat update.
        """
        if self.tracking is None:
            return

        record = WorkerHeartbeatRecord()
        record.worker_id = self.worker_id
        record.slot = self.slot
        record.queue = self.queue
        record.last_heartbeat_at = time.time()

        self.tracking.set_heartbeat(record)

    # heartbeat loop runs independently from task execution
    def _heartbeat_loop(self) -> None:
        """
        Periodically send worker heartbeat in a background thread.
        """
        while not self._heartbeat_stop_event.is_set():
            try:
                self._send_heartbeat()
            except Exception as exc:
                self._log(
                    f"heartbeat_error worker_id={self.worker_id} "
                    f"error_type={type(exc).__name__} error={exc}"
                )

            self._heartbeat_stop_event.wait(self.heartbeat_interval)

    # start heartbeat thread once when worker starts
    def _start_heartbeat_thread(self) -> None:
        """
        Start the background heartbeat thread.
        """
        if self.tracking is None:
            return

        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return

        self._heartbeat_stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.worker_id}-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    # stop heartbeat thread during worker shutdown
    def _stop_heartbeat_thread(self) -> None:
        """
        Stop the background heartbeat thread.
        """
        self._heartbeat_stop_event.set()

        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=self.heartbeat_interval + 1)
            self._heartbeat_thread = None

    # set active task record
    def _set_active_task(
        self,
        task_id: str,
        task_name: str,
        attempt: int,
        max_retries: int,
        started_at: float,
    ) -> None:
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

    def _clear_active_task(self) -> None:
        if self.tracking is None:
            return

        self.tracking.clear_active(self.worker_id, self.slot)

    def _build_dead_letter_message(
        self,
        message: TaskMessage,
        dead_letter_reason: str,
        failure_type: str,
    ) -> DLQMessage:
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
        self.running = True
        self._log(
            f"worker_started worker_id={self.worker_id} "
            f"queue={self.queue} poll_timeout={self.poll_timeout} "
            f"visibility_timeout={self.visibility_timeout} slot={self.slot}"
        )

        # start background heartbeat thread before entering main loop
        self._start_heartbeat_thread()

        try:
            while self.running:
                try:
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
        finally:
            # stop heartbeat thread before clearing heartbeat
            self._stop_heartbeat_thread()

            # clear heartbeat on exit
            if self.tracking is not None:
                self.tracking.clear_heartbeat(self.worker_id, self.slot)

    def stop(self) -> None:
        self.running = False
        self._log(f"worker_stop_requested worker_id={self.worker_id}")

    def _handle_task(self, raw_message: str, message: TaskMessage) -> None:
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

        self._set_active_task(
            task_id=task_id,
            task_name=task_name,
            attempt=attempt,
            max_retries=max_retries,
            started_at=started_at,
        )

        self._log(
            f"task_started worker_id={self.worker_id} "
            f"slot={self.slot} pid={os.getpid()} "
            f"task_id={task_id} task_name={task_name} "
            f"attempt={attempt} max_retries={max_retries}"
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
            self._clear_active_task()