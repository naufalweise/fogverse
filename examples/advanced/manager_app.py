import asyncio
import os
import uuid
from fogverse.manager import FogManager
from components.frame_producer import FrameProducer
from components.frame_processor import FrameProcessor
from components.result_consumer import ResultConsumer

async def main():
    """Configure and run the application using FogManager"""
    
    # Generate a unique app ID for this run
    app_id = os.getenv("APP_ID", str(uuid.uuid4()))
    
    # Configure Kafka settings
    kafka_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
    
    # Initialize components
    producer = FrameProducer(video_source=0)  # Use camera (or specify a video file)
    processor = FrameProcessor()
    consumer = ResultConsumer()
    
    # Create the manager
    manager = FogManager(
        app_id=app_id,
        components=[producer, processor, consumer],
        kafka_servers=kafka_servers,
        topic_name="fogverse-commands",
        log_dir="logs"
    )
    
    # Optional: Configure component deployment
    # This would be more relevant in a distributed setting
    to_deploy = {
        "frame-processor": {
            "app_id": "frame-processor",
            "image": "fogverse/frame-processor:latest",
            "env": {
                "CONSUMER_TOPIC": "raw-frames",
                "PRODUCER_TOPIC": "processed-frames"
            },
            "wait_to_start": False  # Already handled by components list
        }
    }
    
    try:
        # Run the manager
        print(f"Starting Fogverse application with ID: {app_id}")
        await manager.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        # Send shutdown signal
        await manager.send_shutdown()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up
        cv2.destroyAllWindows()
        # Stop the manager
        await manager.stop()
        print("Application stopped")

if __name__ == "__main__":
    asyncio.run(main())
