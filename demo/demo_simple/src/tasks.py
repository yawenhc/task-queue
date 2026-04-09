from __future__ import annotations

from .config import QUEUE_NAME
from .radish_app import app


def compute_sum(limit: int) -> int:
    total = 0
    for i in range(1, limit + 1):
        total += i
    return total


@app.task(
    name="compute_simple_sum",
    queue=QUEUE_NAME,
    max_retries=0,
    retry_delay_ms=0,
)
def compute_simple_sum(limit: int) -> dict:
    total = compute_sum(limit)
    return {
        "limit": limit,
        "sum": total,
    }