from __future__ import annotations

import time

from .config import TASK_COUNT, UPPER_BOUND
from .tasks import compute_sum


def main() -> None:
    start = time.time()

    results = []
    for _ in range(TASK_COUNT):
        result = compute_sum(UPPER_BOUND)
        results.append(result)

    end = time.time()

    print("Baseline execution summary")
    print("--------------------------")
    print(f"Task count: {TASK_COUNT}")
    print(f"Upper bound per task: {UPPER_BOUND}")
    print(f"Completed tasks: {len(results)}")
    print(f"Total execution time: {end - start:.2f} seconds")


if __name__ == "__main__":
    main()