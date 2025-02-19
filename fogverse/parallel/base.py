from abc import ABC, abstractmethod
from typing import Any, Callable

class ParallelTask(ABC):
    """Abstract base class for parallelizable tasks."""
    
    @abstractmethod
    async def process(self, data: Any) -> Any:
        """Processes input data in parallel."""
        pass

class ParallelContext:
    """Manages context for parallel execution."""
    
    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers

    async def __aenter__(self):
        self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown(wait=True)
