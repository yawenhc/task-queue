import time
import uuid
from typing import Callable, Any, Optional, Dict

from radish.backend.redis_backend import RedisBackend
from radish.broker.redis_broker import RedisBroker
from radish.result import AsyncResult
from radish.task import Task
from radish.models import TaskMessage, ResultMessage, TaskState


class Radish:
    """
    Main entry point of the Radish framework.

    Responsibilities:
        1. Hold broker and backend instances
        2. Keep the task registry
        3. Provide the @app.task decorator
        4. Build and send TaskMessage objects
        5. Return AsyncResult objects for result lookup
    """

    def __init__(
        self,
        broker_url: str,
        backend_url: str,
        default_queue: str = "default",
        result_expire_seconds: Optional[int] = None,
        dlq_max_length: Optional[int] = None,
    ):
        """
        Create the core Radish application object.

        Args:
            broker_url:
                Redis URL for the broker, for example:
                redis://localhost:6379/0

            backend_url:
                Redis URL for the result backend, for example:
                redis://localhost:6379/1

            default_queue:
                The default queue name used when a task does not specify one.

            result_expire_seconds:
                Optional TTL for stored result keys in Redis.
        """
        self.default_queue = default_queue

        # Create the broker used for task delivery.
        self.broker = RedisBroker(broker_url,dlq_max_length)

        # Create the backend used for state and result storage.
        self.backend = RedisBackend(
            redis_url=backend_url,
            expire_seconds=result_expire_seconds,
        )

        # Task registry:
        # task_name -> Task object
        self.registry: Dict[str, Task] = {}

    def task(
        self,
        name: Optional[str] = None,
        queue: Optional[str] = None,
        max_retries: int = 2,
        retry_delay_ms: int = 0,
    ) -> Callable[[Callable[..., Any]], Task]:
        """
        Register a Python function as a Radish task.

        This method is designed to be used as a decorator.

        Example:
            @app.task()
            def add(x, y):
                return x + y

        Args:
            name:
                Optional custom task name.
                If not provided, the function name will be used.

            queue:
                Optional queue name.
                If not provided, app.default_queue will be used.

            max_retries:
                Number of retries allowed after the first failure.

            retry_delay_ms:
                Delay before retrying a failed task.

        Returns:
            A decorator function that takes the original Python function
            and returns a Task object.
        """
        def decorator(func: Callable[..., Any]) -> Task:
            task_name = name or func.__name__
            task_queue = queue or self.default_queue

            # Wrap the original function into a Task object.
            task_obj = Task(
                app=self,
                func=func,
                name=task_name,
                queue=task_queue,
                max_retries=max_retries,
                retry_delay_ms=retry_delay_ms,
            )

            # Register the task so workers can resolve task_name -> callable task.
            self.registry[task_name] = task_obj

            return task_obj

        return decorator

    def send_task(
        self,
        task_name: str,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        queue: Optional[str] = None,
        max_retries: int = 2,
        retry_delay_ms: int = 0,
    ) -> AsyncResult:
        """
        Build a TaskMessage and send it to the broker.

        This method is the low-level task sending API.
        It is used internally by Task.delay(), and can also be used directly.

        Args:
            task_name:
                The registered task name.

            args:
                Positional arguments for the task function.

            kwargs:
                Keyword arguments for the task function.

            queue:
                Optional queue override.
                If not provided, app.default_queue will be used.

            max_retries:
                Number of retries allowed after the first failure.

            retry_delay_ms:
                Delay before retrying a failed task.

        Returns:
            AsyncResult:
                A result handle that can be used to query task status/result.
        """
        if kwargs is None:
            kwargs = {}

        target_queue = queue or self.default_queue
        task_id = str(uuid.uuid4())

        # Build the message that will be serialized and pushed to Redis.
        message: TaskMessage = {
            "id": task_id,
            "task_name": task_name,
            "args": list(args),
            "kwargs": kwargs,
            "queue": target_queue,
            "created_at": time.time(),
            "reserved_at": None,
            "attempt": 1,
            "max_retries": max_retries,
            "retry_delay_ms": retry_delay_ms,
        }

        # Write initial task state before it is consumed by a worker.
        self.backend.set_pending(
            task_id=task_id,
            attempt=1,
            max_retries=max_retries,
        )

        # Push the task into the broker ready queue.
        self.broker.enqueue(message)

        # Return a result handle so the caller can poll the backend later.
        return AsyncResult(task_id=task_id, backend=self.backend)

    def get_task(self, task_name: str) -> Optional[Task]:
        """
        Look up a task object from the registry by task name.

        Args:
            task_name:
                The registered task name.

        Returns:
            Task or None
        """
        return self.registry.get(task_name)

    def get_async_result(self, task_id: str) -> AsyncResult:
        """
        Create an AsyncResult handle for an existing task id.

        This is useful when the caller already has a task_id and wants
        to query the task state or result later.
        """
        return AsyncResult(task_id=task_id, backend=self.backend)

    def get_result(self, task_id: str) -> Optional[ResultMessage]:
        """
        Fetch the raw result record for a task from the backend.

        Returns:
            ResultMessage or None
        """
        return self.backend.get_result(task_id)

    def get_task_state(self, task_id: str) -> Optional[TaskState]:
        """
        Fetch only the current task state from the backend.

        Returns:
            TaskState or None if the task result does not exist.
        """
        result = self.backend.get_result(task_id)
        if result is None:
            return None
        return result["state"]