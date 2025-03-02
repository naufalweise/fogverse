import asyncio
import os
import cv2
import numpy as np
from fogverse.consumer import ConsumerStorage
from fogverse.producer import KafkaProducer
from fogverse.runnable import Runnable
from fogverse.utils.data import numpy_to_bytes, bytes_to_numpy

# Set environment variables.
os.environ["PRODUCER_SERVERS"] = "localhost:9092"
os.environ["PRODUCER_TOPIC"] = "processed-frames"

class OpenCVConsumer(ConsumerStorage, Runnable):
    """Captures frames from a video source"""
    
    def __init__(self, device_id=0):
        super().__init__(keep_messages=False)
        self.cap = cv2.VideoCapture(device_id)
        
    async def receive(self):
        """Capture a frame from the camera"""
        success, frame = self.cap.read()
        if not success:
            print("Failed to capture frame.")
            return None
        return frame
    
    async def close_consumer(self):
        """Release the camera when done"""
        if hasattr(self, "cap") and self.cap.isOpened():
            self.cap.release()

class FrameProcessor(ConsumerStorage, KafkaProducer, Runnable):
    """Processes video frames and sends them to Kafka"""
    
    def __init__(self):
        ConsumerStorage.__init__(self)
        KafkaProducer.__init__(self)
        
    def process(self, frame):
        """Process the frame (example: apply grayscale)"""
        if frame is None:
            return None
            
        # Convert to grayscale then back to BGR for visualization.
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        processed = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
        
        # Display the processed frame.
        cv2.imshow('Processed Frame %s.', processed)
        cv2.waitKey(1)
        
        return processed
    
    def encode(self, frame):
        """Convert NumPy array to bytes"""
        if frame is None:
            return b''
        return numpy_to_bytes(frame)

async def main():
    """Set up and run the image processing pipeline"""
    try:
        # Create components.
        camera_consumer = OpenCVConsumer(device_id=0)
        processor = FrameProcessor()
        
        # Connect consumer to processor.
        processor.consumer = camera_consumer
        
        # Start both components.
        await asyncio.gather(
            camera_consumer.run(),
            processor.run()
        )
        
    except KeyboardInterrupt:
        print("Stopping...")
        cv2.destroyAllWindows()

if __name__ == "__main__":
    asyncio.run(main())
