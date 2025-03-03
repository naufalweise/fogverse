import asyncio
import cv2
import numpy as np
import os
import uuid
import time
from fogverse.producer import KafkaProducer
from fogverse.runnable import Runnable
from fogverse.profiler import Profiling
from fogverse.utils.data import numpy_to_bytes

class FrameProducer(KafkaProducer, Runnable):
    """Captures frames from a video source and sends them to Kafka"""
    
    def __init__(self, video_source=0, loop=None):
        self.video_source = video_source
        self.frame_count = 0
        super().__init__(loop=loop or asyncio.get_event_loop())
        
        # Initialize profiling
        self.profile = Profiling(
            name=f"FrameProducer_{os.getenv('APP_ID', str(uuid.uuid4()))}",
            remote_logging=True
        )
        
        # Initialize video capture
        self.cap = cv2.VideoCapture(self.video_source)
        if not self.cap.isOpened():
            raise ValueError(f"Could not open video source {self.video_source}")
    
    async def start_producer(self):
        """Start the Kafka producer and profiling"""
        await super().start_producer()
        await self.profile.start_producer()
        
    async def close_producer(self):
        """Stop the Kafka producer, profiling, and release video capture"""
        if hasattr(self, 'cap') and self.cap.isOpened():
            self.cap.release()
        await self.profile.stop_producer()
        await super().close_producer()
    
    async def receive(self):
        """Capture a frame from the video source"""
        await self.profile._before_receive()
        
        success, frame = self.cap.read()
        if not success:
            if isinstance(self.video_source, int):  # Camera source
                print("Failed to read from camera. Retrying...")
                await asyncio.sleep(0.1)
                return await self.receive()
            else:  # Video file ended
                print("End of video file")
                await asyncio.sleep(1)  # Wait before retrying
                self.cap = cv2.VideoCapture(self.video_source)
                return await self.receive()
        
        self.frame_count += 1
        await self.profile._after_receive(frame)
        return frame
    
    def encode(self, frame):
        """Encode the frame to bytes"""
        self.profile._before_encode(frame)
        encoded = numpy_to_bytes(frame)
        self.profile._after_encode(encoded)
        return encoded
    
    def _get_extra_callback_args(self):
        """Provide additional info for profiling callbacks"""
        timestamp = int(time.time() * 1000)
        headers = [
            ("frame", str(self.frame_count).encode()),
            ("timestamp", str(timestamp).encode())
        ]
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
