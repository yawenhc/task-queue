from radish.backend.redis_backend import RedisBackend

backend = RedisBackend("redis://localhost:6379/1", expire_seconds=3600)

backend.set_started("task-1", attempt=1, max_retries=1)
print(backend.get_result("task-1"))

backend.set_success("task-1", attempt=1, max_retries=1, result_value=3)
print(backend.get_result("task-1"))