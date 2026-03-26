from abc import ABC, abstractmethod
from typing import List, Optional

from radish.models import ActiveTaskRecord


class WorkerTracking(ABC):
    """
    Abstract base class for tracking active tasks executed by workers.

    This component is responsible for storing the current execution state
    of workers (i.e., which task each worker slot is currently executing).

    It is intentionally lightweight and only tracks active tasks,
    not historical execution data.
    """

    @abstractmethod
    def set_active(self, record: ActiveTaskRecord) -> None:
        """
        Mark a worker slot as actively executing a task.

        This should be called right before a worker starts executing a task.
        """
        pass

    @abstractmethod
    def clear_active(self, worker_id: str, slot: int) -> None:
        """
        Clear the active task record for a worker slot.

        This should be called in all exit paths:
        - success
        - failure
        - DLQ
        - retry

        This ensures no stale active records remain.
        """
        pass

    @abstractmethod
    def get_active(self, worker_id: str, slot: int) -> Optional[ActiveTaskRecord]:
        """
        Retrieve the active task for a specific worker slot.

        Returns None if no active task is recorded.
        """
        pass

    @abstractmethod
    def list_all_active(self) -> List[ActiveTaskRecord]:
        """
        Retrieve all active task records.

        This is typically used by:
        - monitor
        - supervisor (optional)
        """
        pass