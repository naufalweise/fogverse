from .base import ScalingPolicy
from typing import Dict, Any

class SpikeDetectionPolicy(ScalingPolicy):
    """Scales based on sudden spikes in metrics."""

    def __init__(self, spike_threshold: float = 1.5, max_replicas: int = 10):
        self.spike_threshold = spike_threshold
        self.max_replicas = max_replicas
        self.previous_metrics = None

    def should_scale(self, metrics: Dict[str, Any]) -> bool:
        """Determines if scaling is needed based on spike detection."""
        if self.previous_metrics is None:
            self.previous_metrics = metrics
            return False

        current_value = metrics.get('messages_per_second', 0)
        previous_value = self.previous_metrics.get('messages_per_second', 0)
        spike_ratio = current_value / previous_value if previous_value > 0 else 0

        self.previous_metrics = metrics
        return spike_ratio > self.spike_threshold

    def target_replicas(self, current: int, metrics: Dict[str, Any]) -> int:
        """Calculates target replicas based on spike detection."""
        return min(current * 2, self.max_replicas)
