import json
import multiprocessing
import signal
import time
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple

from radish.logger import create_supervisor_logger
from radish.models import ActiveTaskRecord, TaskMessage, DLQMessage
from radish.worker.child_runner import run_worker_process, load_app
from radish.worker.redis_worker_tracking import RedisWorkerTracking


@dataclass
class WorkerSlot:
    slot_id: int
    worker_id: str
    process: multiprocessing.Process
    started_at: float


class WorkerSupervisor:
    def __init__(
        self,
        app_path: str,
        queue: str,
        worker_count: int,
        poll_timeout: int,
        visibility_timeout: int,
        project_name: str,
        run_id: str,
        monitor_interval: float = 1.0,
        restart_window_seconds: int = 300,
        max_crashes_in_window: int = 2,
        worker_tracking_redis_url: Optional[str] = None,
        heartbeat_timeout: int = 10,
        heartbeat_interval: int = 2,
    ) -> None:
        self.app_path = app_path
        self.queue = queue
        self.worker_count = worker_count
        self.poll_timeout = poll_timeout
        self.visibility_timeout = visibility_timeout
        self.project_name = project_name
        self.run_id = run_id
        self.monitor_interval = monitor_interval

        # Task crash protection config
        self.restart_window_seconds = restart_window_seconds
        self.max_crashes_in_window = max_crashes_in_window

        self.running = False
        self.shutting_down = False
        self.workers: Dict[int, WorkerSlot] = {}

        # Track recent crash timestamps by task_id
        self.crash_history: Dict[str, List[float]] = {}

        self.logger = create_supervisor_logger(
            project_name=self.project_name,
            run_id=self.run_id,
        )

        # Load app in supervisor for broker/backend access
        self.app = load_app(self.app_path)

        # Infer worker tracking Redis URL if not provided
        self.worker_tracking_redis_url = (
            worker_tracking_redis_url or self._infer_redis_url_from_broker()
        )

        # Create worker tracking client for supervisor side lookup
        self.tracking = RedisWorkerTracking(self.worker_tracking_redis_url)
        self.heartbeat_timeout = heartbeat_timeout
        self.heartbeat_interval = heartbeat_interval

    def _log(self, event: str) -> None:
        self.logger.info(event)

    def _build_worker_id(self, slot_id: int) -> str:
        return f"worker-{slot_id}"

    # Infer Redis URL from app broker connection settings
    def _infer_redis_url_from_broker(self) -> str:
        """
        Build a Redis URL from the broker client's connection settings.

        This allows worker tracking to reuse the same Redis instance
        without requiring an extra explicit URL in the common case.
        """
        connection_kwargs = self.app.broker.redis.connection_pool.connection_kwargs

        host = connection_kwargs.get("host", "localhost")
        port = connection_kwargs.get("port", 6379)
        db = connection_kwargs.get("db", 0)
        username = connection_kwargs.get("username")
        password = connection_kwargs.get("password")

        auth_part = ""
        if username is not None and password is not None:
            auth_part = f"{username}:{password}@"
        elif password is not None:
            auth_part = f":{password}@"
        elif username is not None:
            auth_part = f"{username}@"

        return f"redis://{auth_part}{host}:{port}/{db}"

    # Keep only crash timestamps within the configured window
    def _prune_crash_history(self, task_id: str, now: float) -> None:
        history = self.crash_history.get(task_id, [])
        self.crash_history[task_id] = [
            ts for ts in history if now - ts <= self.restart_window_seconds
        ]

    # Record one crash event for a task and return current crash count in window
    def _record_crash(self, task_id: str) -> int:
        now = time.time()
        self._prune_crash_history(task_id, now)

        history = self.crash_history.get(task_id, [])
        history.append(now)
        self.crash_history[task_id] = history

        return len(history)

    # Determine whether a task has entered crash loop
    def _is_crash_loop(self, task_id: str) -> bool:
        history = self.crash_history.get(task_id, [])
        return len(history) >= self.max_crashes_in_window

    # Find the currently reserved task message in processing queue by task_id
    def _find_processing_message(self, task_id: str) -> Optional[Tuple[str, TaskMessage]]:
        processing_key = self.app.broker._processing_key(self.queue)
        raw_messages = self.app.broker.redis.lrange(processing_key, 0, -1)

        for raw_message in raw_messages:
            message: TaskMessage = json.loads(raw_message)
            if message["id"] == task_id:
                return raw_message, message

        return None

    # Build DLQ message for task crash loop
    def _build_dead_letter_message(self, message: TaskMessage) -> DLQMessage:
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
            "dead_letter_reason": "task_crash_loop",
            "failure_type": "worker_crash",
            "dead_lettered_at": time.time(),
        }

    # Move the active task to DLQ when the task enters crash loop
    def _dead_letter_active_task_for_crash_loop(
        self,
        slot_id: int,
        worker_id: str,
        exitcode: int,
    ) -> None:
        active_record = self.tracking.get_active(worker_id, slot_id)
        if active_record is None:
            self._log(
                f"task_crash_loop_no_active_task slot={slot_id} "
                f"worker_id={worker_id} exitcode={exitcode}"
            )
            return

        found = self._find_processing_message(active_record.task_id)
        if found is None:
            self._log(
                f"task_crash_loop_processing_message_missing slot={slot_id} "
                f"worker_id={worker_id} task_id={active_record.task_id} "
                f"exitcode={exitcode}"
            )
            self.tracking.clear_active(worker_id, slot_id)
            self.tracking.clear_heartbeat(worker_id, slot_id)
            return

        raw_message, message = found
        dlq_message = self._build_dead_letter_message(message)

        self.app.broker.push_to_dlq(dlq_message)
        self.app.backend.set_dead_lettered(
            task_id=message["id"],
            attempt=message["attempt"],
            max_retries=message["max_retries"],
            error=f"worker exited unexpectedly with exitcode={exitcode}",
            dead_letter_reason="task_crash_loop",
            started_at=active_record.started_at,
        )
        self.app.broker.ack(self.queue, raw_message)
        self.tracking.clear_active(worker_id, slot_id)
        self.tracking.clear_heartbeat(worker_id, slot_id)

        self._log(
            f"task_dead_lettered_for_task_crash_loop slot={slot_id} "
            f"worker_id={worker_id} task_id={message['id']} "
            f"task_name={message['task_name']} exitcode={exitcode}"
        )

    # Requeue active task immediately when worker process exits
    def _requeue_active_task_for_worker_exit(
        self,
        slot_id: int,
        worker_id: str,
        exitcode: int,
    ) -> None:
        active_record = self.tracking.get_active(worker_id, slot_id)

        if active_record is None:
            self._log(
                f"worker_exit_no_active slot={slot_id} "
                f"worker_id={worker_id} exitcode={exitcode}"
            )
            return

        found = self._find_processing_message(active_record.task_id)

        if found is None:
            self._log(
                f"worker_exit_processing_missing slot={slot_id} "
                f"worker_id={worker_id} task_id={active_record.task_id} "
                f"exitcode={exitcode}"
            )
            self.tracking.clear_active(worker_id, slot_id)
            self.tracking.clear_heartbeat(worker_id, slot_id)
            return

        raw_message, message = found

        # Requeue immediately
        self.app.broker.requeue(
            queue=self.queue,
            raw_message=raw_message,
            updated_message=message,
        )

        self.tracking.clear_active(worker_id, slot_id)
        self.tracking.clear_heartbeat(worker_id, slot_id)

        self._log(
            f"task_requeued_due_to_worker_exit slot={slot_id} "
            f"worker_id={worker_id} task_id={message['id']} "
            f"exitcode={exitcode}"
        )

    def _spawn_worker(self, slot_id: int) -> WorkerSlot:
        worker_id = self._build_worker_id(slot_id)

        process = multiprocessing.Process(
            target=run_worker_process,
            kwargs={
                "app_path": self.app_path,
                "queue": self.queue,
                "poll_timeout": self.poll_timeout,
                "visibility_timeout": self.visibility_timeout,
                "worker_id": worker_id,
                "project_name": self.project_name,
                "run_id": self.run_id,
                "slot": slot_id,
                "worker_tracking_redis_url": self.worker_tracking_redis_url,
                "heartbeat_interval": self.heartbeat_interval,
            },
            name=worker_id,
        )
        process.start()

        slot = WorkerSlot(
            slot_id=slot_id,
            worker_id=worker_id,
            process=process,
            started_at=time.time(),
        )

        self._log(
            f"worker_spawned slot={slot_id} "
            f"worker_id={worker_id} pid={process.pid}"
        )
        return slot

    def _start_initial_workers(self) -> None:
        for slot_id in range(1, self.worker_count + 1):
            slot = self._spawn_worker(slot_id)
            self.workers[slot_id] = slot

    def _restart_worker(self, slot_id: int) -> None:
        old_slot = self.workers.get(slot_id)

        if old_slot:
            self._log(
                f"worker_restarting slot={slot_id} "
                f"old_pid={old_slot.process.pid} "
                f"exitcode={old_slot.process.exitcode}"
            )

        new_slot = self._spawn_worker(slot_id)
        self.workers[slot_id] = new_slot

    def _handle_exited_worker(self, slot_id: int, slot: WorkerSlot) -> None:
        process = slot.process
        exitcode = process.exitcode

        if exitcode is None:
            return

        if self.shutting_down:
            self._log(
                f"worker_exited_during_shutdown slot={slot_id} "
                f"worker_id={slot.worker_id} pid={process.pid} "
                f"exitcode={exitcode}"
            )
            return

        if exitcode == 0:
            self._log(
                f"worker_exited slot={slot_id} "
                f"worker_id={slot.worker_id} pid={process.pid} exitcode=0"
            )
            self._restart_worker(slot_id)
            return

        self._log(
            f"worker_crashed slot={slot_id} "
            f"worker_id={slot.worker_id} pid={process.pid} exitcode={exitcode}"
        )

        active_record = self.tracking.get_active(slot.worker_id, slot_id)

        # No active task means there is nothing to DLQ or requeue
        if active_record is None:
            self._log(
                f"worker_crash_no_active_task slot={slot_id} "
                f"worker_id={slot.worker_id} exitcode={exitcode}"
            )
            self.tracking.clear_heartbeat(slot.worker_id, slot_id)
            self._restart_worker(slot_id)
            return

        crash_count = self._record_crash(active_record.task_id)
        if self._is_crash_loop(active_record.task_id):
            self._log(
                f"task_crash_loop_detected slot={slot_id} "
                f"worker_id={slot.worker_id} task_id={active_record.task_id} "
                f"crash_count={crash_count} "
                f"window_seconds={self.restart_window_seconds}"
            )

            self._dead_letter_active_task_for_crash_loop(
                slot_id=slot_id,
                worker_id=slot.worker_id,
                exitcode=exitcode,
            )
            self._restart_worker(slot_id)
            return

        self._requeue_active_task_for_worker_exit(
            slot_id=slot_id,
            worker_id=slot.worker_id,
            exitcode=exitcode,
        )

        self._restart_worker(slot_id)

    def _check_workers(self) -> None:
        for slot_id, slot in list(self.workers.items()):
            process = slot.process

            if process.is_alive():
                continue

            process.join(timeout=0.1)
            self._handle_exited_worker(slot_id, slot)

    def _terminate_worker(self, slot: WorkerSlot, timeout: float = 5.0) -> None:
        process = slot.process

        if not process.is_alive():
            process.join(timeout=0.1)
            return

        process.terminate()
        process.join(timeout=timeout)

    def _shutdown_all_workers(self) -> None:
        self.shutting_down = True
        self._log("supervisor_shutdown_started")

        for slot in list(self.workers.values()):
            self._terminate_worker(slot)

        self._log("supervisor_shutdown_finished")

    def _handle_signal(self, signum: int, frame: Optional[object]) -> None:
        self._log(f"signal_received signal={signum}")
        self.running = False

    def _is_heartbeat_expired(self, last_heartbeat_at: float) -> bool:
        """
        Check whether heartbeat is expired.
        """
        return (time.time() - last_heartbeat_at) > self.heartbeat_timeout

    def _handle_offline_worker(self, worker_id: str, slot_id: int) -> None:
        """
        Handle worker that missed heartbeat.
        """
        active_record = self.tracking.get_active(worker_id, slot_id)

        if active_record is None:
            # No active task, just clear heartbeat
            self.tracking.clear_heartbeat(worker_id, slot_id)
            self._log(
                f"worker_offline_no_active slot={slot_id} worker_id={worker_id}"
            )
            return

        found = self._find_processing_message(active_record.task_id)

        if found is None:
            self._log(
                f"worker_offline_processing_missing slot={slot_id} "
                f"worker_id={worker_id} task_id={active_record.task_id}"
            )
            self.tracking.clear_active(worker_id, slot_id)
            self.tracking.clear_heartbeat(worker_id, slot_id)
            return

        raw_message, message = found

        # Requeue the task
        self.app.broker.requeue(
            queue=self.queue,
            raw_message=raw_message,
            updated_message=message,
        )

        self.tracking.clear_active(worker_id, slot_id)
        self.tracking.clear_heartbeat(worker_id, slot_id)

        self._log(
            f"task_requeued_due_to_worker_offline slot={slot_id} "
            f"worker_id={worker_id} task_id={message['id']}"
        )

    def _check_worker_heartbeats(self) -> None:
        """
        Scan all heartbeats and detect offline workers.
        """
        heartbeats = self.tracking.list_all_heartbeats()

        for hb in heartbeats:
            # Process state takes precedence over heartbeat state
            slot = self.workers.get(hb.slot)
            if slot is not None and slot.process.is_alive():
                continue

            if self._is_heartbeat_expired(hb.last_heartbeat_at):
                self._log(
                    f"worker_heartbeat_expired slot={hb.slot} "
                    f"worker_id={hb.worker_id}"
                )
                self._handle_offline_worker(hb.worker_id, hb.slot)

    def start(self) -> None:
        self.running = True

        signal.signal(signal.SIGINT, self._handle_signal)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._handle_signal)

        self._log(
            f"supervisor_started app={self.app_path} "
            f"queue={self.queue} workers={self.worker_count} "
            f"restart_window_seconds={self.restart_window_seconds} "
            f"max_crashes_in_window={self.max_crashes_in_window}"
        )

        self._start_initial_workers()

        try:
            while self.running:
                self._check_workers()

                # Add heartbeat check
                self._check_worker_heartbeats()

                time.sleep(self.monitor_interval)

        except KeyboardInterrupt:
            self._log("keyboard_interrupt")
            self.running = False
        finally:
            self._shutdown_all_workers()