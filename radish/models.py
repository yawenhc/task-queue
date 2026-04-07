# task message
# result message
# task state
from typing import TypedDict, Any, List, Dict
from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    DEAD_LETTERED = "DEAD_LETTERED"


class TaskMessage(TypedDict):
    id: str
    task_name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    queue: str
    created_at: float
    reserved_at: float | None
    attempt: int # start from 1
    max_retries: int # default 1
    retry_delay_ms: int

# result in backend
class ResultMessage(TypedDict):
    id: str
    state: TaskState
    result: Any | None
    error: str | None
    started_at: float | None
    finished_at: float | None
    attempt: int
    max_retries: int
    dead_letter_reason: str | None
    dead_lettered_at: float | None

    requeue_count: int
    last_requeued_at: float | None

    updated_at: float

class DLQMessage(TypedDict):
    id: str
    task_name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    queue: str
    created_at: float
    attempt: int
    max_retries: int
    retry_delay_ms: int

    # DLQ
    dead_letter_reason: str  # "max_tries_exceeded" | "worker_crash_loop"
    failure_type: str        # "normal_failure" | "worker_crash"
    dead_lettered_at: float

class ActiveTaskRecord:
    task_id: str
    task_name: str
    queue: str
    worker_id: str
    slot: int
    attempt: int
    max_retries: int
    started_at: int  # timestamp

class WorkerHeartbeatRecord:
    worker_id: str
    slot: int
    queue: str
    last_heartbeat_at: float