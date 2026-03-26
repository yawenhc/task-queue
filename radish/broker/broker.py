from abc import ABC, abstractmethod
from typing import Optional, Tuple, List

from radish.models import TaskMessage, DLQMessage


class Broker(ABC):
    @abstractmethod
    def enqueue(self, message: TaskMessage) -> None:
        pass

    @abstractmethod
    def reserve(self, queue: str, timeout: int = 5) -> Optional[Tuple[str, TaskMessage]]:
        pass

    @abstractmethod
    def ack(self, queue: str, raw_message: str) -> None:
        pass

    @abstractmethod
    def requeue(self, queue: str, raw_message: str, updated_message: TaskMessage) -> None:
        pass

    @abstractmethod
    def requeue_stale_tasks(self, queue: str, visibility_timeout: int) -> int:
        pass


    @abstractmethod
    def push_to_dlq(self, message: DLQMessage) -> None:
        pass


    @abstractmethod
    def list_dlq(self, queue: str, limit: int = 100) -> List[DLQMessage]:
        pass


    @abstractmethod
    def delete_dlq_task(self, queue: str, task_id: str) -> bool:
        pass


    @abstractmethod
    def requeue_dlq_task(self, queue: str, task_id: str) -> Optional[TaskMessage]:
        pass