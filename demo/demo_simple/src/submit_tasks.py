from __future__ import annotations

import time
from collections import Counter

from .config import POLL_INTERVAL_SECONDS, TASK_COUNT, UPPER_BOUND
from .radish_app import app


TERMINAL_STATES = {"SUCCESS", "FAILURE", "DEAD_LETTERED"}


def main() -> None:
    print(f"Submitting {TASK_COUNT} simple compute tasks...")

    task_ids: list[str] = []

    start = time.time()

    for index in range(TASK_COUNT):
        async_result = app.send_task(
            "compute_simple_sum",
            args=(UPPER_BOUND,),
            queue="simple",
            max_retries=0,
            retry_delay_ms=0,
        )
        task_ids.append(async_result.task_id)
        print(f"Submitted task {index + 1}/{TASK_COUNT}, task_id={async_result.task_id}")

    print("All tasks submitted. Waiting for completion...")

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

        time.sleep(POLL_INTERVAL_SECONDS)

    end = time.time()

    state_counter = Counter(final_states.values())

    print()
    print("Batch execution summary")
    print("-----------------------")
    print(f"Total tasks: {len(task_ids)}")
    print(f"SUCCESS: {state_counter.get('SUCCESS', 0)}")
    print(f"FAILURE: {state_counter.get('FAILURE', 0)}")
    print(f"DEAD_LETTERED: {state_counter.get('DEAD_LETTERED', 0)}")
    print(f"Total batch completion time: {end - start:.2f} seconds")


if __name__ == "__main__":
    main()