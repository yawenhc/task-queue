# radish/monitor.py

"""
This module provides a simple CLI monitor for Radish.

Current capabilities:
1. Show all discovered queues
2. Show ready and processing counts for each queue
3. Show all discovered tasks from the backend
4. Show task state, retry info, and error info
5. Show a placeholder for worker monitoring

Current limitations:
1. Worker heartbeat is not implemented yet
2. Task-to-worker mapping is not implemented yet
"""

import json
import time
from collections import Counter
from typing import Any, Dict, List

import redis


# Default Redis URLs for the current demo project.
BROKER_URL = "redis://localhost:6379/0"
BACKEND_URL = "redis://localhost:6379/1"

# Refresh interval for the monitor screen.
REFRESH_SECONDS = 2

# Redis key prefixes used by the current broker/backend implementation.
QUEUE_PREFIX = "radish:queue:"
PROCESSING_PREFIX = "radish:processing:"
RESULT_PREFIX = "radish:result:"
# Maximum number of task rows shown in the task table.
MAX_TASKS_DISPLAY = 20

def decode_value(value: Any) -> Any:
    """
    Decode Redis bytes into Python strings when needed.

    Args:
        value:
            A value returned by redis-py.

    Returns:
        A decoded string if the input is bytes.
        Otherwise, return the original value unchanged.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def clear_screen() -> None:
    """
    Clear the terminal screen using ANSI escape codes.
    """
    print("\033[2J\033[H", end="")


def create_redis_clients() -> tuple[redis.Redis, redis.Redis]:
    """
    Create Redis clients for broker and backend.

    Returns:
        A tuple of (broker_client, backend_client).
    """
    broker_client = redis.from_url(BROKER_URL)
    backend_client = redis.from_url(BACKEND_URL)
    return broker_client, backend_client


def discover_queue_names(broker_client: redis.Redis) -> List[str]:
    """
    Discover all queue names from both ready and processing keys.

    Args:
        broker_client:
            Redis client connected to the broker database.

    Returns:
        A sorted list of logical queue names.
    """
    queue_names = set()

    for key in broker_client.scan_iter(f"{QUEUE_PREFIX}*"):
        redis_key = decode_value(key)
        queue_name = redis_key.removeprefix(QUEUE_PREFIX)
        queue_names.add(queue_name)

    for key in broker_client.scan_iter(f"{PROCESSING_PREFIX}*"):
        redis_key = decode_value(key)
        queue_name = redis_key.removeprefix(PROCESSING_PREFIX)
        queue_names.add(queue_name)

    return sorted(queue_names)


def get_queue_stats(broker_client: redis.Redis, queue_name: str) -> Dict[str, Any]:
    """
    Read queue statistics for one logical queue.

    Args:
        broker_client:
            Redis client connected to the broker database.

        queue_name:
            The logical queue name, such as 'default'.

    Returns:
        A dictionary with queue statistics.
    """
    ready_key = f"{QUEUE_PREFIX}{queue_name}"
    processing_key = f"{PROCESSING_PREFIX}{queue_name}"

    ready_count = broker_client.llen(ready_key)
    # print("ready_count=", ready_count)
    # print("ready_key =", ready_key, "type =", decode_value(broker_client.type(ready_key)))
    # print("processing_key =", processing_key, "type =", decode_value(broker_client.type(processing_key)))
    processing_count = broker_client.llen(processing_key)



    return {
        "queue_name": queue_name,
        "ready_count": ready_count,
        "processing_count": processing_count,
        "total_visible": ready_count + processing_count,
    }


def parse_task_record(raw_value: str, redis_key: str) -> Dict[str, Any]:
    """
    Convert a Redis string task record into a normalized dictionary.

    Current schema assumption:
    - The Redis key is in the form 'radish:result:{task_id}'
    - The Redis value is a JSON string

    Args:
        raw_value:
            Raw string value returned by Redis.
            It is expected to be a JSON string.

        redis_key:
            The Redis key for this task result.

    Returns:
        A normalized task record dictionary.
    """
    parsed = json.loads(raw_value)

    task_id = redis_key.removeprefix(RESULT_PREFIX)

    return {
        "task_id": str(parsed.get("task_id", task_id)),
        "task_name": str(parsed.get("task_name", "-")),
        "state": str(parsed.get("state", "-")),
        "attempt": str(parsed.get("attempt", "0")),
        "max_retries": str(parsed.get("max_retries", "0")),
        "error": str(parsed.get("error", "")),
        "started_at": str(parsed.get("started_at", "")),
        "finished_at": str(parsed.get("finished_at", "")),
    }



def discover_tasks(backend_client: redis.Redis) -> List[Dict[str, Any]]:
    """
    Discover all task result records from the backend.

    Args:
        backend_client:
            Redis client connected to the backend database.

    Returns:
        A list of normalized task records.
    """
    tasks: List[Dict[str, Any]] = []

    for key in backend_client.scan_iter(f"{RESULT_PREFIX}*"):
        redis_key = decode_value(key)
        #print("redis_key=", redis_key)

        try:
            key_type = decode_value(backend_client.type(key))
        except Exception:
            continue

        if key_type != "string":
            continue

        try:
            raw_value = backend_client.get(key)
        except Exception:
            continue

        if raw_value is None:
            continue

        decoded_value = decode_value(raw_value)

        try:
            task_record = parse_task_record(decoded_value, redis_key)
        except Exception:
            continue

        tasks.append(task_record)

    tasks.sort(key=lambda item: item["task_id"])
    #print("discover tasks=", tasks)
    return tasks


def summarize_task_states(tasks: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Count how many tasks exist in each state.

    Args:
        tasks:
            A list of normalized task records.

    Returns:
        A dictionary mapping state name to count.
    """
    counter = Counter()

    for task in tasks:
        counter[task["state"]] += 1

    return dict(sorted(counter.items()))


def format_retry_text(task: Dict[str, Any]) -> str:
    """
    Format retry information for display.

    Args:
        task:
            One normalized task record.

    Returns:
        A readable retry text such as '1/3'.
    """
    return f"{task['attempt']}/{task['max_retries']}"


def format_error_text(error: str, max_length: int = 40) -> str:
    """
    Truncate long error messages for table display.

    Args:
        error:
            Original error text.

        max_length:
            Maximum displayed length.

    Returns:
        A shortened error string.
    """
    if not error:
        return "-"
    if len(error) <= max_length:
        return error
    return error[: max_length - 3] + "..."

def parse_timestamp(value: str) -> float:
    """
    Convert a timestamp string into a sortable float.

    Args:
        value:
            Timestamp text stored in a task record.

    Returns:
        A float timestamp if conversion succeeds.
        Returns -1.0 when the value is missing or invalid.
    """
    if not value:
        return -1.0

    try:
        return float(value)
    except Exception:
        return -1.0

def format_timestamp_for_display(value: str) -> str:
    """
    Format a timestamp string into a short readable display string.

    Args:
        value:
            Timestamp text stored in a task record.

    Returns:
        A short display string for the timestamp.
        Returns '-' if the value is missing or invalid.
    """
    timestamp = parse_timestamp(value)
    if timestamp < 0:
        return "-"

    try:
        return time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(timestamp)
        )
    except Exception:
        return "-"

def sort_tasks_for_display(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort tasks for monitor display.

    Display priority:
    1. Unfinished tasks first: PENDING, STARTED, RETRYING
    2. Then finished tasks: SUCCESS, FAILURE
    3. Within each group, sort by started_at in descending order

    Args:
        tasks:
            A list of normalized task records.

    Returns:
        A new sorted task list for display.
    """
    unfinished_states = {"PENDING", "STARTED", "RETRYING"}

    def sort_key(task: Dict[str, Any]):
        is_finished = 1
        if task.get("state") in unfinished_states:
            is_finished = 0

        started_at_value = parse_timestamp(task.get("started_at", ""))

        return (is_finished, -started_at_value, task.get("task_id", ""))

    return sorted(tasks, key=sort_key)

def print_header() -> None:
    """
    Print the monitor title section.
    """
    print("=" * 88)
    print("RADISH MONITOR")
    print("=" * 88)
    print(f"Broker URL : {BROKER_URL}")
    print(f"Backend URL: {BACKEND_URL}")
    print(f"Refresh    : {REFRESH_SECONDS}s")
    print("")


def print_workers_section() -> None:
    """
    Print the current worker section.

    Worker monitoring is not implemented yet, so this section
    explicitly explains the current limitation.
    """
    print("Workers")
    print("-" * 88)
    print("Not available yet.")
    print("Reason: worker heartbeat and task-to-worker mapping are not implemented.")
    print("")


def print_queues_section(queue_stats: List[Dict[str, Any]]) -> None:
    """
    Print queue overview information.

    Args:
        queue_stats:
            A list of queue statistics dictionaries.
    """
    print("Queues")
    print("-" * 88)

    if not queue_stats:
        print("No queues found.")
        print("")
        return

    print(
        f"{'queue_name':<20}"
        f"{'ready':<12}"
        f"{'processing':<14}"
        f"{'total_visible':<14}"
    )

    for item in queue_stats:
        print(
            f"{item['queue_name']:<20}"
            f"{item['ready_count']:<12}"
            f"{item['processing_count']:<14}"
            f"{item['total_visible']:<14}"
        )

    print("")


def print_task_summary_section(tasks: List[Dict[str, Any]]) -> None:
    """
    Print task summary counts by state.

    Args:
        tasks:
            A list of normalized task records.
    """
    print("Task Summary")
    print("-" * 88)

    if not tasks:
        print("No task records found.")
        print("")
        return

    state_summary = summarize_task_states(tasks)
    summary_text = ", ".join(
        f"{state}={count}" for state, count in state_summary.items()
    )

    print(f"total_tasks={len(tasks)}")
    print(summary_text)
    print(f"display_limit={MAX_TASKS_DISPLAY} most relevant tasks")
    print("")


def print_tasks_section(tasks: List[Dict[str, Any]]) -> None:
    """
    Print detailed task records.

    Args:
        tasks:
            A list of normalized task records.
            This list is expected to be pre-sorted and pre-limited
            before being passed to this function.
    """
    print("Tasks")
    print("-" * 88)

    if not tasks:
        print("No task records found.")
        print("")
        return

    print(
        f"{'task_id':<36} "
        f"{'state':<10} "
        f"{'retry':<8} "
        f"{'started':<10} "
        f"{'finished':<10} "
        f"{'error':<12}"
    )

    for task in tasks:
        started_text = format_timestamp_for_display(task["started_at"])
        finished_text = format_timestamp_for_display(task["finished_at"])

        print(
            f"{task['task_id']:<36} "
            f"{task['state']:<10} "
            f"{format_retry_text(task):<8} "
            f"{started_text:<10} "
            f"{finished_text:<10} "
            f"{format_error_text(task['error']):<12}"
        )

    print("")


def print_footer() -> None:
    """
    Print monitor footer hints.
    """
    print("-" * 88)
    print("Current limitations:")
    print("1. Worker status is not available yet.")
    print("2. Task-to-worker ownership is not available yet.")
    print("3. This monitor currently focuses on queues and task states.")
    print("")


def render_monitor_screen(
    broker_client: redis.Redis,
    backend_client: redis.Redis,
) -> None:
    """
    Gather data and render one full monitor screen.

    Args:
        broker_client:
            Redis client connected to the broker database.

        backend_client:
            Redis client connected to the backend database.
    """
    queue_names = discover_queue_names(broker_client)
    queue_stats = [get_queue_stats(broker_client, name) for name in queue_names]

    all_tasks = discover_tasks(backend_client)
    display_tasks = sort_tasks_for_display(all_tasks)[:MAX_TASKS_DISPLAY]

    print_header()
    print_workers_section()
    print_queues_section(queue_stats)
    print_task_summary_section(all_tasks)
    print_tasks_section(display_tasks)
    print_footer()


def main() -> None:
    """
    Start the monitor loop.

    This command continuously refreshes the terminal display.
    """
    broker_client, backend_client = create_redis_clients()

    while True:
        clear_screen()
        render_monitor_screen(broker_client, backend_client)
        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()