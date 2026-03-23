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


class ResultMessage(TypedDict):
    id: str
    state: TaskState
    result: Any | None
    error: str | None
    started_at: float | None
    finished_at: float | None
    attempt: int
    max_retries: int