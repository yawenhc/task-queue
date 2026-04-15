from __future__ import annotations

import time
from collections import Counter

from .config import CITIES_FILE
from .radish_app import app
from .tasks import load_cities


TERMINAL_STATES = {"SUCCESS", "FAILURE", "DEAD_LETTERED"}


def main() -> None:
    cities = load_cities(CITIES_FILE)

    print(f"Submitting {len(cities)} weather tasks...")

    async_results = []
    task_ids = []

    submit_start = time.time()

    for city in cities:
        async_result = app.send_task(
            "fetch_weather_for_city",
            args=(city,),
            max_retries=2,#2
        )
        async_results.append(async_result)
        task_ids.append(async_result.task_id)
        print(f"Submitted city={city}, task_id={async_result.task_id}")

    print("All tasks submitted. Waiting for completion...")

    poll_interval_seconds = 2.0
    final_states: dict[str, str] = {}

    while True:
        finished_count = 0

        for task_id in task_ids:
            result = app.get_result(task_id)

            if result is None:
                continue

            state_value = result["state"]
            state_name = state_value.value if hasattr(state_value, "value") else str(state_value)

            if state_name in TERMINAL_STATES:
                final_states[task_id] = state_name
                finished_count += 1

        print(f"Progress: {finished_count}/{len(task_ids)} tasks finished")

        if finished_count == len(task_ids):
            break

        time.sleep(poll_interval_seconds)

    submit_end = time.time()
    total_elapsed = submit_end - submit_start

    state_counter = Counter(final_states.values())

    success_count = state_counter.get("SUCCESS", 0)
    failure_count = state_counter.get("FAILURE", 0)
    dead_lettered_count = state_counter.get("DEAD_LETTERED", 0)

    print()
    print("Batch execution summary")
    print("-----------------------")
    print(f"Total tasks: {len(task_ids)}")
    print(f"SUCCESS: {success_count}")
    print(f"FAILURE: {failure_count}")
    print(f"DEAD_LETTERED: {dead_lettered_count}")
    print(f"Total batch completion time: {total_elapsed:.2f} seconds")


if __name__ == "__main__":
    main()

