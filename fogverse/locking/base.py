from abc import ABC, abstractmethod

class DistributedLock(ABC):
    """Abstract base class for distributed lock implementations."""
    
    @abstractmethod
    async def acquire(self, resource: str, timeout: int) -> bool:
        """Attempts to acquire a lock for the specified resource."""
        pass

    @abstractmethod
    async def release(self, resource: str):
        """Releases an acquired lock."""
        pass
