import time
from typing import Any, Optional

from radish.models import TaskState
from radish.backend.redis_backend import RedisBackend


class AsyncResult:
    """
    AsyncResult represents the handle to an asynchronous task result.

    It allows users to:
        - check task state
        - wait for completion
        - fetch result
        - inspect errors
        - delete stored result
    """

    def __init__(self, task_id: str, backend: RedisBackend):
        """
        Initialize the result handle.

        Args:
            task_id:
                Unique identifier of the task.

            backend:
                Backend instance responsible for result storage.
        """
        self.task_id = task_id
        self.backend = backend

    def status(self) -> TaskState:
        """
        Return the current task state.

        Example return values:
            PENDING
            STARTED
            RETRYING
            SUCCESS
            FAILURE
        """
        result = self.backend.get_result(self.task_id)

        if result is None:
            return TaskState.PENDING

        return result["state"]

    def ready(self) -> bool:
        """
        Return True if the task has finished execution.
        """
        state = self.status()
        return state in (TaskState.SUCCESS, TaskState.FAILURE)

    def successful(self) -> bool:
        """
        Return True if the task finished successfully.
        """
        return self.status() == TaskState.SUCCESS

    def failed(self) -> bool:
        """
        Return True if the task finished with failure.
        """
        return self.status() == TaskState.FAILURE

    def error(self) -> Optional[str]:
        """
        Return error message if task failed.
        """
        result = self.backend.get_result(self.task_id)

        if result is None:
            return None

        return result["error"]

    def get(self, timeout: Optional[float] = None, interval: float = 0.5) -> Any:
        """
        Wait until the task finishes and return the result.

        Args:
            timeout:
                Maximum seconds to wait. None means wait forever.

            interval:
                Polling interval for checking task status.

        Returns:
            Task result if successful.

        Raises:
            Exception if task failed.
            TimeoutError if timeout reached.
        """
        start_time = time.time()

        while True:
            result = self.backend.get_result(self.task_id)

            if result is not None:
                state = result["state"]

                if state == TaskState.SUCCESS:
                    return result["result"]

                if state == TaskState.FAILURE:
                    raise Exception(result["error"])

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    raise TimeoutError("Task result timeout")

            time.sleep(interval)

    def forget(self) -> None:
        """
        Delete the stored result from backend.

        This removes the result key from Redis.
        """
        self.backend.delete_result(self.task_id)

    def __repr__(self) -> str:
        """
        Return readable representation of the result handle.
        """
        return f"<AsyncResult task_id={self.task_id}>"