from radish import Radish

from .config import (
    BACKEND_URL,
    BROKER_URL,
    DLQ_MAX_LENGTH,
    QUEUE_NAME,
    RESULT_EXPIRE_SECONDS,
)

app = Radish(
    broker_url=BROKER_URL,
    backend_url=BACKEND_URL,
    default_queue=QUEUE_NAME,
    result_expire_seconds=RESULT_EXPIRE_SECONDS,
    dlq_max_length=DLQ_MAX_LENGTH,
)