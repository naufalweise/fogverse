import asyncio
import cv2
import numpy as np
import os
import uuid
import time
from fogverse.consumer import KafkaConsumer
from fogverse.producer import KafkaProducer
from fogverse.runnable import Runnable
from fogverse.profiling import Profiling
from fogverse.utils.data import bytes_to_numpy, numpy_to_bytes, get_header

class FrameProcessor(KafkaConsumer, KafkaProducer, Runnable):
    """Processes frames from Kafka and sends results back to Kafka"""
    
    def __init__(self, loop=None):
        KafkaConsumer.__init__(self, loop=loop or asyncio.get_event_loop())
        KafkaProducer.__init__(self, loop=self._loop)
        
        # Initialize profiling
        self.profile = Profiling(
            name=f"FrameProcessor_{os.getenv('APP_ID', str(uuid.uuid4()))}",
            remote_logging=True
        )
        
        # Auto-decode and auto-encode features
        self.auto_decode = True
        self.auto_encode = True
    
    async def start_producer(self):
        """Start the Kafka producer and profiling"""
        await KafkaProducer.start_producer(self)
        await self.profile.start_producer()
    
    async def start_consumer(self):
        """Start the Kafka consumer"""
        await KafkaConsumer.start_consumer(self)
    
    async def close_producer(self):
        """Stop the Kafka producer and profiling"""
        await self.profile.stop_producer()
        await KafkaProducer.close_producer(self)
    
    async def close_consumer(self):
        """Stop the Kafka consumer"""
        await KafkaConsumer.close_consumer(self)
    
    def decode(self, data):
        """Decode the raw bytes to a numpy array"""
        self.profile._before_decode(data)
        if isinstance(data, bytes):
            image_data = bytes_to_numpy(data)
            if image_data.ndim == 1:  # If it's a 1D array, decode it as an image
                decoded = cv2.imdecode(image_data, cv2.IMREAD_COLOR)
            else:
                decoded = image_data
        else:
            decoded = data
        self.profile._after_decode(decoded)
        return decoded
    
    def process(self, frame):
        """Process the frame (example: detect edges)"""
        self.profile._before_process(frame)
        
        # Simple edge detection as an example
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 100, 200)
        processed = cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR)
        
        # Add frame info as text overlay
        frame_number = get_header(self.message.headers, "frame", "-1")
        cv2.putText(
            processed, 
            f"Frame: {frame_number}", 
            (10, 30), 
            cv2.FONT_HERSHEY_SIMPLEX, 
            1, 
            (0, 255, 0), 
            2
        )
        
        self.profile._after_process(processed)
        return processed
    
    def encode(self, frame):
        """Encode the processed frame to bytes"""
        self.profile._before_encode(frame)
        encoded = numpy_to_bytes(frame)
        self.profile._after_encode(encoded)
        return encoded
    
    def _get_extra_callback_args(self):
        """Provide additional info for profiling callbacks"""
        # Pass through the original frame number and timestamp
        headers = [h for h in self.message.headers]
        return {
            "log_data": self.profile._log_data.copy(),
            "headers": headers,
            "topic": self.producer_topic,
            "timestamp": self.profile._datetime_before_send
        }
    
    async def callback(self, record_metadata, log_data=None, headers=None, topic=None, timestamp=None):
        """Callback function after sending a message"""
        await self.profile.callback(
            record_metadata, 
            log_data=log_data, 
            headers=headers, 
            topic=topic, 
            timestamp=timestamp
        )
