import json
import time
from typing import Optional, Any, Dict
from redis import Redis

from radish.models import ResultMessage, TaskState


class RedisBackend:
    def __init__(self, redis_url: str, expire_seconds: Optional[int] = None):
        """
        redis_url example:
        redis://localhost:6379/1

        expire_seconds:
        If set, result keys will expire after this many seconds.
        """
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.expire_seconds = expire_seconds

    def _result_key(self, task_id: str) -> str:
        return f"radish:result:{task_id}"

    def set_result(self, result: ResultMessage) -> None:
        key = self._result_key(result["id"])
        payload = json.dumps(result)
        if self.expire_seconds is None:
            self.redis.set(key, payload)
        else:
            self.redis.setex(key, self.expire_seconds, payload)

    def get_result(self, task_id: str) -> Optional[ResultMessage]:
        key = self._result_key(task_id)
        raw = self.redis.get(key)
        if raw is None:
            return None

        data: Dict[str, Any] = json.loads(raw)

        state_value = data.get("state")
        if isinstance(state_value, str):
            data["state"] = TaskState(state_value)

        return data  # type: ignore[return-value]

    def set_pending(self, task_id: str, attempt: int, max_retries: int) -> None:
        now = time.time()
        result: ResultMessage = {
            "id": task_id,
            "state": TaskState.PENDING,
            "result": None,
            "error": None,
            "started_at": None,
            "finished_at": None,
            "attempt": attempt,
            "max_retries": max_retries,
        }
        self.set_result(result)

    def set_started(self, task_id: str, attempt: int, max_retries: int) -> None:
        now = time.time()
        result: ResultMessage = {
            "id": task_id,
            "state": TaskState.STARTED,
            "result": None,
            "error": None,
            "started_at": now,
            "finished_at": None,
            "attempt": attempt,
            "max_retries": max_retries,
        }
        self.set_result(result)

    def set_retrying(self, task_id: str, attempt: int, max_retries: int, error: str) -> None:
        now = time.time()
        result: ResultMessage = {
            "id": task_id,
            "state": TaskState.RETRYING,
            "result": None,
            "error": error,
            "started_at": now,
            "finished_at": None,
            "attempt": attempt,
            "max_retries": max_retries,
        }
        self.set_result(result)

    def set_success(
        self,
        task_id: str,
        attempt: int,
        max_retries: int,
        result_value: Any,
        started_at: Optional[float] = None,
    ) -> None:
        now = time.time()
        result: ResultMessage = {
            "id": task_id,
            "state": TaskState.SUCCESS,
            "result": result_value,
            "error": None,
            "started_at": started_at,
            "finished_at": now,
            "attempt": attempt,
            "max_retries": max_retries,
        }
        self.set_result(result)

    def set_failure(
        self,
        task_id: str,
        attempt: int,
        max_retries: int,
        error: str,
        started_at: Optional[float] = None,
    ) -> None:
        now = time.time()
        result: ResultMessage = {
            "id": task_id,
            "state": TaskState.FAILURE,
            "result": None,
            "error": error,
            "started_at": started_at,
            "finished_at": now,
            "attempt": attempt,
            "max_retries": max_retries,
        }
        self.set_result(result)