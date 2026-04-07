from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class WorkerView:
    worker_id: str
    status: str
    current_task_id: str | None
    current_task_name: str | None
    attempt: int | None
    queue: str | None
    started_at: str | None
    last_heartbeat_at: str | None
    seconds_since_heartbeat: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkerOverviewView:
    total_workers: int
    busy_workers: int
    idle_workers: int
    offline_workers: int
    workers: list[WorkerView]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["workers"] = [worker.to_dict() for worker in self.workers]
        return data


@dataclass
class TaskView:
    task_id: str
    task_name: str | None
    state: str
    attempt: int | None
    max_retries: int | None
    created_at: str | None
    started_at: str | None
    finished_at: str | None
    worker_id: str | None
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TasksPageView:
    total_tasks: int
    state_counts: dict[str, int]
    ready_queue_length: int
    processing_queue_length: int
    recent_tasks: list[TaskView]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["recent_tasks"] = [task.to_dict() for task in self.recent_tasks]
        return data