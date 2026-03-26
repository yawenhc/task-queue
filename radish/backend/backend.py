from abc import ABC, abstractmethod
from typing import Optional, Any

from radish.models import ResultMessage


class Backend(ABC):
    @abstractmethod
    def set_result(self, result: ResultMessage) -> None:
        pass

    @abstractmethod
    def get_result(self, task_id: str) -> Optional[ResultMessage]:
        pass

    @abstractmethod
    def set_pending(self, task_id: str, attempt: int, max_retries: int) -> None:
        pass

    @abstractmethod
    def set_started(self, task_id: str, attempt: int, max_retries: int) -> None:
        pass

    @abstractmethod
    def set_retrying(
        self,
        task_id: str,
        attempt: int,
        max_retries: int,
        error: str,
    ) -> None:
        pass

    @abstractmethod
    def set_success(
        self,
        task_id: str,
        attempt: int,
        max_retries: int,
        result_value: Any,
        started_at: Optional[float] = None,
    ) -> None:
        pass

    @abstractmethod
    def set_failure(
        self,
        task_id: str,
        attempt: int,
        max_retries: int,
        error: str,
        started_at: Optional[float] = None,
    ) -> None:
        pass

    @abstractmethod
    def cleanup(self) -> int:
        """
        Clean up expired or obsolete backend data.
        Return the number of deleted records/keys.
        """
        pass

    @abstractmethod
    def requeue_to_pending(
            self,
            task_id: str,
            max_retries: int,
    ) -> None:
        pass

    @abstractmethod
    def set_dead_lettered(
            self,
            task_id: str,
            attempt: int,
            max_retries: int,
            error: str,
            dead_letter_reason: str,
            started_at: Optional[float] = None,
    ) -> None:
        pass