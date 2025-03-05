import time
import cv2
import numpy as np
from fogverse import Runnable, FogProfiler
from fogverse.consumer import OpenCVConsumer
from fogverse.producer import KafkaProducer
from fogverse.utils.data import get_config, numpy_to_bytes

class SmartCamera(OpenCVConsumer, KafkaProducer, FogProfiler):
    def __init__(self):
        self._device = get_config('CAMERA_INDEX', cls=int, default=0)
        self.profiling_topic = get_config('PROFILING_TOPIC')
        OpenCVConsumer.__init__(self, device=self._device)
        KafkaProducer.__init__(self)
        FogProfiler.__init__(self, topic=self.profiling_topic)
        self.frame_count = 0

    async def process(self, frame):
        # Preprocessing pipeline
        processed = cv2.resize(frame, (640, 480))
        processed = cv2.cvtColor(processed, cv2.COLOR_BGR2RGB)
        return processed

    async def send(self, data):
        headers = [
            ('frame_id', str(self.frame_count).encode()),
            ('timestamp', str(int(time.time() * 1000)).encode())
        ]
        await super().send(numpy_to_bytes(data), 
                         topic=get_config('CAMERA_TOPIC'),
                         headers=headers)
        self.frame_count += 1

    async def run(self):
        await self.start()
        try:
            while True:
                frame = await self.receive()
                if frame is None: continue
                
                with self.profiler_context():
                    processed = await self.process(frame)
                    await self.send(processed)
        finally:
            await self.close()

if __name__ == '__main__':
    SmartCamera().run()
