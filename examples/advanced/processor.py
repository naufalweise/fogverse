import cv2
import numpy as np
from fogverse.profiler import FogProfiler
from fogverse.consumer import KafkaConsumer
from fogverse.producer import KafkaProducer
from fogverse.utils.data import get_config
from fogverse.utils.image import get_config, bytes_to_numpy, numpy_to_bytes

class AIProcessor(KafkaConsumer, KafkaProducer, FogProfiler):
    def __init__(self):
        self.consumer_topic = get_config('CAMERA_TOPIC')
        self.producer_topic = get_config('PROCESSED_TOPIC')
        self.profiling_topic = get_config('PROFILING_TOPIC')
        
        KafkaConsumer.__init__(self)
        KafkaProducer.__init__(self)
        FogProfiler.__init__(self, topic=self.profiling_topic)
        
        # Initialize model
        self.model = self._load_edge_detection_model()

    def _load_edge_detection_model(self):
        """Load a simple edge detection model using OpenCV."""

        # This is a placeholder for a real model. Replace with your actual model.
        def edge_detection(frame):
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 100, 200)
            return edges
        return edge_detection

    async def process(self, frame):
        """Process a frame using the loaded model."""
        # Preprocess the frame (resize and normalize).
        resized_frame = cv2.resize(frame, (640, 480))
        normalized_frame = resized_frame / 255.0

        # Apply the model (edge detection in this case).
        processed_frame = self.model(normalized_frame)

        # Convert back to uint8 for visualization.
        processed_frame = (processed_frame * 255).astype(np.uint8)
        return processed_frame

    async def _receive(self):
        msg = await super().receive()
        frame = bytes_to_numpy(msg.value)
        return frame, msg.headers

    async def send(self, data, headers):
        await super().send(numpy_to_bytes(data), 
                         topic=self.producer_topic,
                         headers=headers)

    async def run(self):
        await self.start()
        try:
            while True:
                frame, headers = await self._receive()
                
                with self.profiler_context():
                    processed = await self.process(frame)
                    await self.send(processed, headers)
        finally:
            await self.close()

if __name__ == '__main__':
    AIProcessor().run()
