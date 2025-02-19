from fogverse.consumer import KafkaConsumer
from fogverse.auto_scaling import AutoScaler

class ScaledConsumer:
    """Consumer that dynamically scales based on workload."""
    
    def __init__(self, scaler: AutoScaler):
        self.scaler = scaler
        self.consumer = KafkaConsumer(topics=['workload'])

    async def run(self):
        while True:
            message = await self.consumer.receive()
            metrics = self._collect_metrics()
            new_replicas = await self.scaler.evaluate(
                'image-processor', 
                current_replicas=2, 
                metrics=metrics
            )
            self._scale_replicas(new_replicas)

    def _collect_metrics(self) -> Dict[str, Any]:
        # Collect metrics from Kafka or monitoring system
        return {'messages_per_second': 1500}

    def _scale_replicas(self, count: int):
        # Trigger scaling action (e.g., Kubernetes API)
        print(f"Scaling to {count} replicas")
