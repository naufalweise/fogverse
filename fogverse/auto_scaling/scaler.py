import asyncio
from typing import Dict, Any
from .policies.base import ScalingPolicy

class AutoScaler:
    """Manages auto-scaling decisions and actions."""
    
    def __init__(self, policies: Dict[str, ScalingPolicy]):
        self.policies = policies

    async def evaluate(self, 
                      component_id: str,
                      current_replicas: int,
                      metrics: Dict[str, Any]) -> int:
        """Evaluates scaling needs across all policies."""
        decisions = [current_replicas]
        for policy in self.policies.values():
            if policy.should_scale(metrics):
                decisions.append(
                    policy.target_replicas(current_replicas, metrics)
                )
        return max(decisions)
