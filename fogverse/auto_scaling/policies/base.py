from abc import ABC, abstractmethod
from typing import Dict, Any

class ScalingPolicy(ABC):
    """Base class for auto-scaling decision policies."""
    
    @abstractmethod
    def should_scale(self, metrics: Dict[str, Any]) -> bool:
        """Determines if scaling should occur based on metrics."""
        pass

    @abstractmethod
    def target_replicas(self, current: int, metrics: Dict[str, Any]) -> int:
        """Calculates desired number of replicas."""
        pass
