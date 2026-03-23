from typing import Callable, Any, Optional

from radish.result import AsyncResult


class Task:
    """
    Task wraps a normal Python function and turns it into
    a Radish task object.

    Responsibilities:
        1. Store task metadata and task configuration
        2. Provide user-facing APIs such as delay()
        3. Forward task sending to app.send_task()
        4. Provide run() for worker-side execution
    """

    def __init__(
        self,
        app: Any,
        func: Callable[..., Any],
        name: str,
        queue: str,
        max_retries: int,
        retry_delay_ms: int,
    ):
        """
        Create a task object from a normal Python function.

        Args:
            app:
                The RadishApp instance that owns this task.

            func:
                The original Python function defined by the user.

            name:
                The task name used in registry lookup and message dispatch.

            queue:
                The default queue name for this task.

            max_retries:
                Number of retries allowed after the first failure.

            retry_delay_ms:
                Delay before retrying a failed task.
        """
        # Keep a reference to the app so this task can call app.send_task().
        self.app = app

        # Keep the original Python function.
        self.func = func

        # Task metadata and default execution options.
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.retry_delay_ms = retry_delay_ms

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """
        Send a task using the task's default configuration.

        This is the most common user-facing API.

        Example:
            result = add.delay(1, 2)

        Args:
            *args:
                Positional arguments for the original task function.

            **kwargs:
                Keyword arguments for the original task function.

        Returns:
            AsyncResult:
                A handle for querying task state and final result.
        """
        return self.app.send_task(
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            queue=self.queue,
            max_retries=self.max_retries,
            retry_delay_ms=self.retry_delay_ms,
        )

    def apply_async(
        self,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        queue: Optional[str] = None,
        max_retries: Optional[int] = None,
        retry_delay_ms: Optional[int] = None,
    ) -> AsyncResult:
        """
        Send a task with optional per-call overrides.

        This is a more configurable version of delay().

        Example:
            result = add.apply_async(
                args=(1, 2),
                queue="math",
                max_retries=3,
                retry_delay_ms=1000,
            )

        Args:
            args:
                Positional arguments for the task function.

            kwargs:
                Keyword arguments for the task function.

            queue:
                Optional queue override for this specific call.

            max_retries:
                Optional retry count override for this specific call.

            retry_delay_ms:
                Optional retry delay override for this specific call.

        Returns:
            AsyncResult:
                A handle for querying task state and final result.
        """
        if kwargs is None:
            kwargs = {}

        target_queue = queue if queue is not None else self.queue
        target_max_retries = (
            max_retries if max_retries is not None else self.max_retries
        )
        target_retry_delay_ms = (
            retry_delay_ms if retry_delay_ms is not None else self.retry_delay_ms
        )

        return self.app.send_task(
            task_name=self.name,
            args=args,
            kwargs=kwargs,
            queue=target_queue,
            max_retries=target_max_retries,
            retry_delay_ms=target_retry_delay_ms,
        )

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the original task function.

        This method is intended for worker-side execution.

        Args:
            *args:
                Positional arguments for the original task function.

            **kwargs:
                Keyword arguments for the original task function.

        Returns:
            Any:
                The return value of the original task function.
        """
        return self.func(*args, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the task locally like a normal function call.

        This does not send anything to the broker.

        Example:
            value = add(1, 2)

        This is useful for local testing and direct execution.
        """
        return self.run(*args, **kwargs)

    def __repr__(self) -> str:
        """
        Return a readable string representation of the task.
        """
        return (
            f"<Task name={self.name} "
            f"queue={self.queue} "
            f"max_retries={self.max_retries} "
            f"retry_delay_ms={self.retry_delay_ms}>"
        )