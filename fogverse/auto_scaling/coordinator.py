from typing import Dict, Any
from .policies.base import ScalingPolicy

class AutoScaler:
    """Orchestrates scaling decisions across multiple policies."""
    
    def __init__(self, policies: Dict[str, ScalingPolicy]):
        self.policies = policies

    async def evaluate_scaling(self, 
                              component_id: str,
                              current_replicas: int,
                              metrics: Dict[str, Any]) -> int:
        """Aggregates scaling decisions from all policies."""
        decisions = []
        for policy_name, policy in self.policies.items():
            if policy.should_scale(metrics):
                decisions.append(
                    policy.target_replicas(current_replicas, metrics)
                )
        return max(decisions, default=current_replicas)
