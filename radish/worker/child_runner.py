import importlib
from typing import Any, Optional

from radish.logger import create_worker_logger
from radish.worker.redis_worker_tracking import RedisWorkerTracking
from radish.worker.worker import Worker

# This version remains self-contained and does not depend on imports.py.
def load_app(app_path: str) -> Any:
    """
    Supports:
        example.tasks
        example.tasks:app

    Default attribute name is 'app'.
    """
    if ":" in app_path:
        module_name, attr_name = app_path.split(":", 1)
    else:
        module_name, attr_name = app_path, "app"

    module = importlib.import_module(module_name)

    if not hasattr(module, attr_name):
        raise ValueError(
            f"Module '{module_name}' does not contain attribute '{attr_name}'."
        )

    return getattr(module, attr_name)


def run_worker_process(
    *,
    app_path: str,
    queue: str,
    poll_timeout: int,
    visibility_timeout: int,
    worker_id: str,
    project_name: str,
    run_id: str,
    slot: int = 0,
    worker_tracking_redis_url: Optional[str] = None,
) -> None:
    """
    Entry point for a worker child process.

    This process:
      - loads the user app
      - creates a dedicated worker logger
      - creates a WorkerTracking instance when configured
      - creates a Worker instance
      - starts the worker loop
    """
    logger = create_worker_logger(
        run_id=run_id,
        project_name=project_name,
        worker_id=worker_id,
        console=True,
    )

    logger.info(
        f"worker_starting worker_id={worker_id} app={app_path} "
        f"queue={queue} slot={slot}"
    )

    try:
        app = load_app(app_path)
        logger.info(f"loaded_tasks worker_id={worker_id} tasks={list(app.registry.keys())}")

        # create worker tracking only when redis url is provided
        tracking = None
        if worker_tracking_redis_url is not None:
            tracking = RedisWorkerTracking(worker_tracking_redis_url)
            logger.info(
                f"worker_tracking_enabled worker_id={worker_id} "
                f"slot={slot}"
            )

        worker = Worker(
            registry=app.registry,
            broker=app.broker,
            backend=app.backend,
            queue=queue,
            poll_timeout=poll_timeout,
            visibility_timeout=visibility_timeout,
            worker_id=worker_id,
            logger=logger,
            tracking=tracking,
            slot=slot,
        )

        logger.info(
            f"queue={queue} poll_timeout={poll_timeout} "
            f"visibility_timeout={visibility_timeout} slot={slot}"
        )

        worker.start()

        logger.info(f"worker_stopped worker_id={worker_id}")

    except KeyboardInterrupt:
        logger.info(f"worker_keyboard_interrupt worker_id={worker_id}")
        raise
    except Exception as exc:
        logger.info(
            f"worker_fatal_error worker_id={worker_id} "
            f"error_type={type(exc).__name__} error={exc}"
        )
        raise