from .base import ScalingPolicy
from typing import Dict, Any

class ThroughputPolicy(ScalingPolicy):
    """Scales based on message throughput metrics."""
    
    def __init__(self, threshold: int = 1000, max_replicas: int = 10):
        self.threshold = threshold
        self.max_replicas = max_replicas

    def should_scale(self, metrics: Dict[str, Any]) -> bool:
        """Determines if scaling is needed based on throughput."""
        return metrics.get('messages_per_second', 0) > self.threshold

    def target_replicas(self, current: int, metrics: Dict[str, Any]) -> int:
        """Calculates target replicas based on load."""
        load_factor = metrics['messages_per_second'] / self.threshold
        return min(int(load_factor * current), self.max_replicas)
