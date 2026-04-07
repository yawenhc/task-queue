from __future__ import annotations

from datetime import datetime, timezone

from radish.monitor.schemas import TaskView, TasksPageView, WorkerOverviewView, WorkerView


class MonitorService:
    def __init__(
        self,
        redis_reader,
        queue: str,
        heartbeat_timeout_seconds: int = 10,
        recent_task_limit: int = 20,
    ) -> None:
        self.redis_reader = redis_reader
        self.queue = queue
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.recent_task_limit = recent_task_limit

    def get_workers_page_data(self) -> dict:
        workers = self._build_worker_views()
        return {
            "queue": self.queue,
            "workers": [worker.to_dict() for worker in workers],
        }

    def get_worker_overview_data(self) -> dict:
        workers = self._build_worker_views()

        busy_workers = sum(1 for worker in workers if worker.status == "busy")
        idle_workers = sum(1 for worker in workers if worker.status == "idle")
        offline_workers = sum(1 for worker in workers if worker.status == "offline")

        overview = WorkerOverviewView(
            total_workers=len(workers),
            busy_workers=busy_workers,
            idle_workers=idle_workers,
            offline_workers=offline_workers,
            workers=workers,
        )
        return overview.to_dict()

    def get_tasks_page_data(self) -> dict:
        raw_tasks = self.redis_reader.get_all_task_records()
        ready_queue_length, processing_queue_length = self.redis_reader.get_queue_lengths()

        state_counts: dict[str, int] = {}
        task_views: list[TaskView] = []

        for record in raw_tasks:
            state = str(record.get("state", "UNKNOWN"))
            state_counts[state] = state_counts.get(state, 0) + 1

            task_views.append(
                TaskView(
                    task_id=str(record.get("id") or record.get("task_id") or ""),
                    task_name=self._nullable_str(record.get("task_name")),
                    state=state,
                    attempt=self._nullable_int(record.get("attempt")),
                    max_retries=self._nullable_int(record.get("max_retries")),
                    created_at=self._format_timestamp(record.get("created_at")),
                    started_at=self._format_timestamp(record.get("started_at")),
                    finished_at=self._format_timestamp(record.get("finished_at")),
                    worker_id=self._nullable_str(record.get("worker_id")),
                    error=self._nullable_str(record.get("error")),
                )
            )

        unfinished_priority = {"PENDING": 0, "STARTED": 0, "RETRYING": 0}

        task_views.sort(
            key=lambda task: (
                unfinished_priority.get(task.state, 1),
                self._sortable_time(task.started_at, task.created_at, task.finished_at),
            ),
            reverse=False,
        )

        unfinished = [task for task in task_views if task.state in unfinished_priority]
        finished = [task for task in task_views if task.state not in unfinished_priority]

        finished.sort(
            key=lambda task: self._sortable_time(task.finished_at, task.started_at, task.created_at),
            reverse=True,
        )

        ordered_tasks = unfinished + finished
        limited_tasks = ordered_tasks[: self.recent_task_limit]

        page = TasksPageView(
            total_tasks=len(task_views),
            state_counts=state_counts,
            ready_queue_length=ready_queue_length,
            processing_queue_length=processing_queue_length,
            recent_tasks=limited_tasks,
        )
        return page.to_dict()

    def _build_worker_views(self) -> list[WorkerView]:
        heartbeat_records = self.redis_reader.get_all_worker_heartbeats()
        active_task_records = self.redis_reader.get_all_active_tasks()

        heartbeat_map: dict[str, dict] = {}
        for record in heartbeat_records:
            worker_id = str(record.get("worker_id") or "")
            if worker_id:
                heartbeat_map[worker_id] = record

        active_map: dict[str, dict] = {}
        for record in active_task_records:
            worker_id = str(record.get("worker_id") or "")
            if worker_id:
                active_map[worker_id] = record

        worker_ids = sorted(set(heartbeat_map.keys()) | set(active_map.keys()))
        now = datetime.now(timezone.utc)

        workers: list[WorkerView] = []
        for worker_id in worker_ids:
            heartbeat = heartbeat_map.get(worker_id, {})
            active_task = active_map.get(worker_id, {})

            # last_heartbeat_raw = heartbeat.get("updated_at") or heartbeat.get("heartbeat_at") or heartbeat.get("timestamp")
            # last_heartbeat_dt = self._parse_timestamp(last_heartbeat_raw)
            # seconds_since_heartbeat = None
            # if last_heartbeat_dt is not None:
            #     seconds_since_heartbeat = int((now - last_heartbeat_dt).total_seconds())
            #
            # status = "offline"
            # if seconds_since_heartbeat is not None and seconds_since_heartbeat <= self.heartbeat_timeout_seconds:
            #     status = "busy" if active_task else "idle"
            last_heartbeat_raw = (
                    heartbeat.get("last_heartbeat_at")
                    or heartbeat.get("updated_at")
                    or heartbeat.get("heartbeat_at")
                    or heartbeat.get("timestamp")
            )
            last_heartbeat_dt = self._parse_timestamp(last_heartbeat_raw)

            seconds_since_heartbeat = None
            if last_heartbeat_dt is not None:
                seconds_since_heartbeat = int((now - last_heartbeat_dt).total_seconds())

            status = "offline"
            if seconds_since_heartbeat is not None and seconds_since_heartbeat <= self.heartbeat_timeout_seconds:
                status = "busy" if active_task else "idle"

            workers.append(
                WorkerView(
                    worker_id=worker_id,
                    status=status,
                    current_task_id=self._nullable_str(active_task.get("task_id")),
                    current_task_name=self._nullable_str(active_task.get("task_name")),
                    attempt=self._nullable_int(active_task.get("attempt")),
                    queue=self._nullable_str(active_task.get("queue") or heartbeat.get("queue")),
                    started_at=self._format_timestamp(active_task.get("started_at")),
                    last_heartbeat_at=self._format_timestamp(last_heartbeat_raw),
                    seconds_since_heartbeat=seconds_since_heartbeat,
                )
            )

        return workers

    def _parse_timestamp(self, value):
        if value in (None, "", 0):
            return None

        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        if isinstance(value, (int, float)):
            if value > 10_000_000_000:
                return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
            return datetime.fromtimestamp(value, tz=timezone.utc)

        if isinstance(value, str):
            try:
                numeric_value = float(value)
                if numeric_value > 10_000_000_000:
                    return datetime.fromtimestamp(numeric_value / 1000, tz=timezone.utc)
                return datetime.fromtimestamp(numeric_value, tz=timezone.utc)
            except ValueError:
                pass

            try:
                normalized = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                return None

        return None

    def _format_timestamp(self, value) -> str | None:
        dt = self._parse_timestamp(value)
        if dt is None:
            return None
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def _sortable_time(self, *values: str | None) -> float:
        for value in values:
            if not value:
                continue
            try:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return dt.timestamp()
            except ValueError:
                continue
        return 0.0

    def _nullable_str(self, value) -> str | None:
        if value in (None, ""):
            return None
        return str(value)

    def _nullable_int(self, value) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None