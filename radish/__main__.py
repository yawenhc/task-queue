import argparse
import sys
from datetime import datetime

from radish.worker.supervisor import WorkerSupervisor
from radish.worker.child_runner import run_worker_process
from radish.monitor.app import run_monitor_server

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="radish")
    parser.add_argument(
        "-A",
        "--app",
        required=True,
        help="Application module path, e.g. example.tasks or example.tasks:app",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("worker", help="Start Radish worker(s)")
    worker_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes to start",
    )
    worker_parser.add_argument(
        "--queue",
        default="default",
        help="Queue name to consume from",
    )
    worker_parser.add_argument(
        "--poll-timeout",
        type=int,
        default=3,
        help="Broker poll timeout in seconds",
    )
    worker_parser.add_argument(
        "--visibility-timeout",
        type=int,
        default=400,
        help="Processing visibility timeout in seconds",
    )
    worker_parser.add_argument(
        "--project-name",
        default="radish",
        help="Project name used in log file naming",
    )

    # Added monitor subcommand
    monitor_parser = subparsers.add_parser("monitor", help="Start Radish monitor")
    monitor_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address for the monitor server",
    )
    monitor_parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port for the monitor server",
    )
    monitor_parser.add_argument(
        "--queue",
        default="default",
        help="Queue name to inspect",
    )

    return parser


def generate_run_id() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "worker":
        if args.workers < 1:
            print("--workers must be >= 1", file=sys.stderr)
            raise SystemExit(2)

        run_id = generate_run_id()

        # if args.workers == 1:
        #     run_worker_process(
        #         app_path=args.app,
        #         queue=args.queue,
        #         poll_timeout=args.poll_timeout,
        #         visibility_timeout=args.visibility_timeout,
        #         worker_id="worker-1",
        #         project_name=args.project_name,
        #         run_id=run_id,
        #     )
        #     return

        supervisor = WorkerSupervisor(
            app_path=args.app,
            queue=args.queue,
            worker_count=args.workers,
            poll_timeout=args.poll_timeout,
            visibility_timeout=args.visibility_timeout,
            project_name=args.project_name,
            run_id=run_id,
        )
        supervisor.start()
        return

    if args.command == "monitor":
        run_monitor_server(
            app_path=args.app,
            queue=args.queue,
            host=args.host,
            port=args.port,
        )
        return

    parser.print_help()
    raise SystemExit(2)


if __name__ == "__main__":
    main()