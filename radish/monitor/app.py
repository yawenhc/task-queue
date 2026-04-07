from __future__ import annotations

from flask import Flask

from radish.monitor.redis_reader import RedisReader
from radish.monitor.routes import register_routes
from radish.monitor.service import MonitorService


def create_monitor_app(app_path: str, queue: str) -> Flask:
    app = Flask(
        __name__,
        template_folder="templates",
    )

    redis_reader = RedisReader(app_path=app_path, queue=queue)
    monitor_service = MonitorService(
        redis_reader=redis_reader,
        queue=queue,
        heartbeat_timeout_seconds=10,
        recent_task_limit=20,
    )

    app.config["MONITOR_SERVICE"] = monitor_service
    register_routes(app)

    return app


def run_monitor_server(app_path: str, queue: str, host: str, port: int) -> None:
    app = create_monitor_app(app_path=app_path, queue=queue)
    print(f"Starting Radish monitor at http://{host}:{port}")
    app.run(host=host, port=port, debug=False)