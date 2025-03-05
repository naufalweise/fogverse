from fogverse import KafkaConsumer
from fogverse.utils.data import get_config
import json

class ProfilingMonitor(KafkaConsumer):
    def __init__(self):
        super().__init__(consumer_topic=get_config('PROFILING_TOPIC'),
                         group_id='profiling-monitor')
        self.metrics = {}

    async def process_metric(self, data):
        component = data['profiler']
        self.metrics.setdefault(component, [])
        self.metrics[component].append(data['log_data'])
        
        if len(self.metrics[component]) > 1000:
            self.metrics[component].pop(0)

    async def run(self):
        await self.start()
        try:
            while True:
                msg = await self.receive()
                data = json.loads(msg.value.decode())
                await self.process_metric(data)
        finally:
            await self.close()

if __name__ == '__main__':
    ProfilingMonitor().run()
