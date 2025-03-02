import asyncio
import cv2
import os
import uuid
from fogverse.consumer import KafkaConsumer
from fogverse.runnable import Runnable
from fogverse.profiling import Profiling
from fogverse.utils.data import bytes_to_numpy, get_header

class ResultConsumer(KafkaConsumer, Runnable):
    """Consumes processed frames and displays them"""
    
    def __init__(self, loop=None):
        super().__init__(loop=loop or asyncio.get_event_loop())
        
        # Initialize profiling
        self.profile = Profiling(
            name=f"ResultConsumer_{os.getenv('APP_ID', str(uuid.uuid4()))}",
            remote_logging=True
        )
        
        # Auto-decode feature
        self.auto_decode = True
    
    async def start_consumer(self):
        """Start the Kafka consumer and profiling"""
        await super().start_consumer()
        await self.profile.start_producer()
    
    async def close_consumer(self):
        """Stop the Kafka consumer and profiling"""
        await self.profile.stop_producer()
        await super().close_consumer()
    
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
        """Display the processed frame"""
        self.profile._before_process(frame)
        
        # Extract metadata
        frame_number = get_header(self.message.headers, "frame", "-1")
        
        # Display the frame
        cv2.imshow(f"Processed Frame (Consumer View)", frame)
        key = cv2.waitKey(1) & 0xFF
        
        # Save the frame periodically
        if int(frame_number) % 30 == 0:  # Save every 30th frame
            os.makedirs("output", exist_ok=True)
            cv2.imwrite(f"output/frame_{frame_number}.jpg", frame)
            print(f"Saved frame {frame_number}")
        
        # Allow exiting with 'q' key
        if key == ord('q'):
            cv2.destroyAllWindows()
            asyncio.create_task(self.send_shutdown())
            
        self.profile._after_process(frame)
        return frame
    
    async def send_shutdown(self):
        """Signal application to shut down"""
        print("Shutting down...")
        # This could signal the manager to gracefully shut down components
        # In a real application, this could publish a shutdown message
        
    async def send(self, data):
        """Override send to finish profiling (no actual sending)"""
        self.profile._before_send(data)
        self.profile._after_send(data)
