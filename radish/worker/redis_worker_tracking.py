import json
import time
from typing import List, Optional

import redis

from radish.models import ActiveTaskRecord
from radish.worker.worker_tracking import WorkerTracking
from radish.models import WorkerHeartbeatRecord



class RedisWorkerTracking(WorkerTracking):
    """
    Redis-based implementation of WorkerTracking.

    Each worker slot stores its active task in a dedicated key:

        radish:worker:active:{worker_id}:{slot}

    The value is a JSON-serialized ActiveTaskRecord.

    This design ensures:
    - O(1) read/write per slot
    - minimal IO overhead
    - simple cleanup logic
    """

    def __init__(self, redis_url: str):
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)

        # Key prefix for active task records
        self.prefix = "radish:worker:active"
        self.heartbeat_prefix = "radish:worker:heartbeat"

    def _key(self, worker_id: str, slot: int) -> str:
        """
        Build Redis key for a worker slot.
        """
        return f"{self.prefix}:{worker_id}:{slot}"

    def _heartbeat_key(self, worker_id: str, slot: int) -> str:
        return f"{self.heartbeat_prefix}:{worker_id}:{slot}"

    def set_active(self, record: ActiveTaskRecord) -> None:
        """
        Store the active task record for a worker slot.
        """
        key = self._key(record.worker_id, record.slot)

        value = json.dumps({
            "task_id": record.task_id,
            "task_name": record.task_name,
            "queue": record.queue,
            "worker_id": record.worker_id,
            "slot": record.slot,
            "attempt": record.attempt,
            "max_retries": record.max_retries,
            "started_at": record.started_at,
        })

        self.client.set(key, value)

    def clear_active(self, worker_id: str, slot: int) -> None:
        """
        Remove the active task record for a worker slot.
        """
        key = self._key(worker_id, slot)
        self.client.delete(key)

    def get_active(self, worker_id: str, slot: int) -> Optional[ActiveTaskRecord]:
        """
        Retrieve the active task record for a specific worker slot.

        Returns None if no active task is recorded.
        """
        key = self._key(worker_id, slot)
        value = self.client.get(key)

        if not value:
            return None

        data = json.loads(value)

        record = ActiveTaskRecord()
        record.task_id = data["task_id"]
        record.task_name = data["task_name"]
        record.queue = data["queue"]
        record.worker_id = data["worker_id"]
        record.slot = data["slot"]
        record.attempt = data["attempt"]
        record.max_retries = data["max_retries"]
        record.started_at = data["started_at"]

        return record

    def list_all_active(self) -> List[ActiveTaskRecord]:
        """
        Retrieve all active task records.

        This performs a Redis key scan on:
            radish:worker:active:*

        Note:
        This is not expected to be high-frequency.
        Intended for monitoring or debugging.
        """
        pattern = f"{self.prefix}:*"

        records: List[ActiveTaskRecord] = []

        for key in self.client.scan_iter(pattern):
            value = self.client.get(key)
            if not value:
                continue

            data = json.loads(value)

            record = ActiveTaskRecord()
            record.task_id = data["task_id"]
            record.task_name = data["task_name"]
            record.queue = data["queue"]
            record.worker_id = data["worker_id"]
            record.slot = data["slot"]
            record.attempt = data["attempt"]
            record.max_retries = data["max_retries"]
            record.started_at = data["started_at"]

            records.append(record)

        return records

    def set_heartbeat(self, record: WorkerHeartbeatRecord) -> None:
        """
        Store heartbeat for a worker slot.
        """
        key = self._heartbeat_key(record.worker_id, record.slot)

        value = json.dumps({
            "worker_id": record.worker_id,
            "slot": record.slot,
            "queue": record.queue,
            "last_heartbeat_at": record.last_heartbeat_at,
        })

        self.client.set(key, value)

    def clear_heartbeat(self, worker_id: str, slot: int) -> None:
        """
        Remove heartbeat record for a worker slot.
        """
        key = self._heartbeat_key(worker_id, slot)
        self.client.delete(key)

    def get_heartbeat(self, worker_id: str, slot: int) -> Optional[WorkerHeartbeatRecord]:
        """
        Retrieve heartbeat for a worker slot.
        """
        key = self._heartbeat_key(worker_id, slot)
        value = self.client.get(key)

        if not value:
            return None

        data = json.loads(value)

        record = WorkerHeartbeatRecord()
        record.worker_id = data["worker_id"]
        record.slot = data["slot"]
        record.queue = data["queue"]
        record.last_heartbeat_at = data["last_heartbeat_at"]

        return record

    def list_all_heartbeats(self) -> List[WorkerHeartbeatRecord]:
        """
        Retrieve all heartbeat records.
        """
        pattern = f"{self.heartbeat_prefix}:*"

        records: List[WorkerHeartbeatRecord] = []

        for key in self.client.scan_iter(pattern):
            value = self.client.get(key)
            if not value:
                continue

            data = json.loads(value)

            record = WorkerHeartbeatRecord()
            record.worker_id = data["worker_id"]
            record.slot = data["slot"]
            record.queue = data["queue"]
            record.last_heartbeat_at = data["last_heartbeat_at"]

            records.append(record)

        return records