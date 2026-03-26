import json
import time
from typing import Optional, Any, Dict
from redis import Redis

from radish.backend.backend import Backend
from radish.models import ResultMessage, TaskState


class RedisBackend(Backend):  # inherit from Backend
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

    # build a complete ResultMessage payload
    def _build_result_message(
        self,
        task_id: str,
        state: TaskState,
        attempt: int,
        max_retries: int,
        result_value: Any = None,
        error: Optional[str] = None,
        started_at: Optional[float] = None,
        finished_at: Optional[float] = None,
        dead_letter_reason: Optional[str] = None,
        dead_lettered_at: Optional[float] = None,
        requeue_count: int = 0,
        last_requeued_at: Optional[float] = None,
        updated_at: Optional[float] = None,
    ) -> ResultMessage:
        now = time.time()

        return {
            "id": task_id,
            "state": state,
            "result": result_value,
            "error": error,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempt": attempt,
            "max_retries": max_retries,
            "dead_letter_reason": dead_letter_reason,
            "dead_lettered_at": dead_lettered_at,
            "requeue_count": requeue_count,
            "last_requeued_at": last_requeued_at,
            "updated_at": now if updated_at is None else updated_at,
        }

    # for backward-compatible defaults
    def _normalize_result_message(self, data: Dict[str, Any]) -> ResultMessage:
        if "dead_letter_reason" not in data:
            data["dead_letter_reason"] = None
        if "dead_lettered_at" not in data:
            data["dead_lettered_at"] = None
        if "requeue_count" not in data:
            data["requeue_count"] = 0
        if "last_requeued_at" not in data:
            data["last_requeued_at"] = None
        if "updated_at" not in data:
            data["updated_at"] = time.time()

        return data  # type: ignore[return-value]

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

        # fill missing fields for older stored records
        return self._normalize_result_message(data)

    def set_pending(self, task_id: str, attempt: int, max_retries: int) -> None:
        # CHANGED: preserve existing requeue metadata if the record already exists
        existing = self.get_result(task_id)
        requeue_count = existing["requeue_count"] if existing is not None else 0
        last_requeued_at = existing["last_requeued_at"] if existing is not None else None

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.PENDING,
            attempt=attempt,
            max_retries=max_retries,
            result_value=None,
            error=None,
            started_at=None,
            finished_at=None,
            dead_letter_reason=None,
            dead_lettered_at=None,
            requeue_count=requeue_count,
            last_requeued_at=last_requeued_at,
        )
        self.set_result(result)

    def set_started(self, task_id: str, attempt: int, max_retries: int) -> None:
        now = time.time()

        # preserve existing requeue metadata if the record already exists
        existing = self.get_result(task_id)
        requeue_count = existing["requeue_count"] if existing is not None else 0
        last_requeued_at = existing["last_requeued_at"] if existing is not None else None

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.STARTED,
            attempt=attempt,
            max_retries=max_retries,
            result_value=None,
            error=None,
            started_at=now,
            finished_at=None,
            dead_letter_reason=None,
            dead_lettered_at=None,
            requeue_count=requeue_count,
            last_requeued_at=last_requeued_at,
        )
        self.set_result(result)

    def set_retrying(self, task_id: str, attempt: int, max_retries: int, error: str) -> None:
        # preserve existing started_at and requeue metadata if available
        existing = self.get_result(task_id)
        started_at = existing["started_at"] if existing is not None else None
        requeue_count = existing["requeue_count"] if existing is not None else 0
        last_requeued_at = existing["last_requeued_at"] if existing is not None else None

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.RETRYING,
            attempt=attempt,
            max_retries=max_retries,
            result_value=None,
            error=error,
            started_at=started_at,
            finished_at=None,
            dead_letter_reason=None,
            dead_lettered_at=None,
            requeue_count=requeue_count,
            last_requeued_at=last_requeued_at,
        )
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

        # preserve existing requeue metadata if the record already exists
        existing = self.get_result(task_id)
        requeue_count = existing["requeue_count"] if existing is not None else 0
        last_requeued_at = existing["last_requeued_at"] if existing is not None else None

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.SUCCESS,
            attempt=attempt,
            max_retries=max_retries,
            result_value=result_value,
            error=None,
            started_at=started_at,
            finished_at=now,
            dead_letter_reason=None,
            dead_lettered_at=None,
            requeue_count=requeue_count,
            last_requeued_at=last_requeued_at,
        )
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

        # preserve existing requeue metadata if the record already exists
        existing = self.get_result(task_id)
        requeue_count = existing["requeue_count"] if existing is not None else 0
        last_requeued_at = existing["last_requeued_at"] if existing is not None else None

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.FAILURE,
            attempt=attempt,
            max_retries=max_retries,
            result_value=None,
            error=error,
            started_at=started_at,
            finished_at=now,
            dead_letter_reason=None,
            dead_lettered_at=None,
            requeue_count=requeue_count,
            last_requeued_at=last_requeued_at,
        )
        self.set_result(result)

    # add DLQ backend state update
    def set_dead_lettered(
        self,
        task_id: str,
        attempt: int,
        max_retries: int,
        error: str,
        dead_letter_reason: str,
        started_at: Optional[float] = None,
    ) -> None:
        now = time.time()

        # preserve existing requeue metadata if the record already exists
        existing = self.get_result(task_id)
        requeue_count = existing["requeue_count"] if existing is not None else 0
        last_requeued_at = existing["last_requeued_at"] if existing is not None else None

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.DEAD_LETTERED,
            attempt=attempt,
            max_retries=max_retries,
            result_value=None,
            error=error,
            started_at=started_at,
            finished_at=now,
            dead_letter_reason=dead_letter_reason,
            dead_lettered_at=now,
            requeue_count=requeue_count,
            last_requeued_at=last_requeued_at,
        )
        self.set_result(result)

    # add requeue path from DLQ back to pending
    def requeue_to_pending(
        self,
        task_id: str,
        max_retries: int,
    ) -> None:
        now = time.time()
        existing = self.get_result(task_id)

        if existing is None:
            requeue_count = 1
        else:
            requeue_count = existing["requeue_count"] + 1

        result = self._build_result_message(
            task_id=task_id,
            state=TaskState.PENDING,
            attempt=1,
            max_retries=max_retries,
            result_value=None,
            error=None,
            started_at=None,
            finished_at=None,
            dead_letter_reason=None,
            dead_lettered_at=None,
            requeue_count=requeue_count,
            last_requeued_at=now,
            updated_at=now,
        )
        self.set_result(result)

    # implement cleanup for non-expiring keys
    def cleanup(self) -> int:
        if self.expire_seconds is not None:
            return 0

        deleted = 0
        cursor = 0

        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match="radish:result:*", count=100)
            for key in keys:
                raw = self.redis.get(key)
                if raw is None:
                    continue

                try:
                    data: Dict[str, Any] = json.loads(raw)
                except Exception:
                    self.redis.delete(key)
                    deleted += 1
                    continue

                updated_at = data.get("updated_at")
                if not isinstance(updated_at, (int, float)):
                    self.redis.delete(key)
                    deleted += 1

            if cursor == 0:
                break

        return deleted