from __future__ import annotations

import importlib
import json
from typing import Any

import redis


class RedisReader:
    def __init__(self, app_path: str, queue: str) -> None:
        self.app_path = app_path
        self.queue = queue

        radish_app = self._load_radish_app(app_path)

        broker_url = self._extract_redis_url(
            radish_app,
            ["broker_url", "broker", "broker_uri"],
        )
        backend_url = self._extract_redis_url(
            radish_app,
            ["backend_url", "backend", "result_backend"],
        )

        # Fallback to default local Redis URLs if app metadata is unavailable
        if broker_url is None:
            broker_url = "redis://localhost:6379/0"
        if backend_url is None:
            backend_url = "redis://localhost:6379/1"

        self.broker_redis = redis.Redis.from_url(
            broker_url,
            decode_responses=True,
        )
        self.backend_redis = redis.Redis.from_url(
            backend_url,
            decode_responses=True,
        )

    def get_all_worker_heartbeats(self) -> list[dict[str, Any]]:
        patterns = [
            "radish:worker:heartbeat:*",  # correct pattern
        ]
        return self._read_records_from_patterns(self.broker_redis, patterns)

    def get_all_active_tasks(self) -> list[dict[str, Any]]:
        patterns = [
            "radish:worker:active:*",
            "radish:worker:active_task:*",
            "radish:active_task:*",
        ]
        return self._read_records_from_patterns(self.broker_redis, patterns)

    def get_all_task_records(self) -> list[dict[str, Any]]:
        patterns = [
            "radish:result:*",
            "radish:results:*",
            "radish:task_result:*",
            "radish:backend:result:*",
        ]
        return self._read_records_from_patterns(self.backend_redis, patterns)

    def get_queue_lengths(self) -> tuple[int, int]:
        ready_key = f"radish:queue:{self.queue}"
        processing_key = f"radish:processing:{self.queue}"

        ready_len = self._safe_len(self.broker_redis, ready_key)
        processing_len = self._safe_len(self.broker_redis, processing_key)

        return ready_len, processing_len

    def _safe_len(self, client: redis.Redis, key: str) -> int:
        key_type = client.type(key)
        if key_type == "list":
            return client.llen(key)
        if key_type == "zset":
            return client.zcard(key)
        if key_type == "set":
            return client.scard(key)
        if key_type == "hash":
            return client.hlen(key)
        if key_type == "string":
            return 1 if client.get(key) is not None else 0
        return 0

    def _read_records_from_patterns(
        self,
        client: redis.Redis,
        patterns: list[str],
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for pattern in patterns:
            for key in client.scan_iter(match=pattern):
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                record = self._read_record(client, key)
                if not record:
                    continue

                if "worker_id" not in record:
                    inferred_worker_id = self._infer_worker_id_from_key(key)
                    if inferred_worker_id:
                        record["worker_id"] = inferred_worker_id

                if "id" not in record and "task_id" not in record:
                    inferred_task_id = self._infer_task_id_from_key(key)
                    if inferred_task_id:
                        record["id"] = inferred_task_id

                records.append(record)

        return records

    def _read_record(self, client: redis.Redis, key: str) -> dict[str, Any] | None:
        key_type = client.type(key)

        if key_type == "string":
            raw_value = client.get(key)
            return self._parse_string_record(raw_value)

        if key_type == "hash":
            raw_hash = client.hgetall(key)
            return dict(raw_hash) if raw_hash else None

        return None

    def _parse_string_record(self, raw_value: str | None) -> dict[str, Any] | None:
        if not raw_value:
            return None

        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, dict):
                return parsed
            return None
        except json.JSONDecodeError:
            return None

    def _infer_worker_id_from_key(self, key: str) -> str | None:
        parts = key.split(":")
        for index, part in enumerate(parts):
            if part in {"worker", "workers", "heartbeat"} and index + 1 < len(parts):
                candidate = parts[index + 1]
                if candidate not in {"heartbeat", "active", "active_task"}:
                    return candidate
        return None

    def _infer_task_id_from_key(self, key: str) -> str | None:
        parts = key.split(":")
        if parts:
            return parts[-1]
        return None

    def _load_radish_app(self, app_path: str):
        # Case 1: explicit attribute path such as example.tasks:app
        if ":" in app_path:
            module_name, attr_name = app_path.split(":", 1)
            module = importlib.import_module(module_name)
            return getattr(module, attr_name, None)

        # Case 2: module path such as example.tasks
        module = importlib.import_module(app_path)

        # Try common app variable names first
        for candidate_name in ("app", "radish_app"):
            candidate = getattr(module, candidate_name, None)
            if candidate is not None:
                return candidate

        # Try to find any object that looks like a Radish app
        for _, value in vars(module).items():
            if hasattr(value, "broker_url") or hasattr(value, "backend_url"):
                return value
            if hasattr(value, "broker") or hasattr(value, "backend"):
                return value

        # Return None instead of raising so monitor can still use fallback URLs
        return None

    def _extract_redis_url(self, app_obj, candidate_attrs: list[str]) -> str | None:
        if app_obj is None:
            return None

        for attr_name in candidate_attrs:
            if not hasattr(app_obj, attr_name):
                continue

            value = getattr(app_obj, attr_name)

            if isinstance(value, str) and value.startswith("redis://"):
                return value

            if hasattr(value, "url"):
                nested_url = getattr(value, "url")
                if isinstance(nested_url, str) and nested_url.startswith("redis://"):
                    return nested_url

        return None