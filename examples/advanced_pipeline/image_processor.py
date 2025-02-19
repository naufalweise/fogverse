import asyncio
from examples.advanced_pipeline.anomaly_detector import AnomalyDetector
from fogverse.consumer import KafkaConsumer
from fogverse.parallel.base import ParallelContext
from fogverse.producer import KafkaProducer
from fogverse.parallel import ParallelTask
from fogverse.utils.logging import FogLogger

class ImageProcessor(ParallelTask):
    """Processes images using parallel execution."""
    
    async def process(self, image_data: bytes) -> bytes:
        # Example: Apply image transformation
        return await self._apply_filter(image_data)

    async def _apply_filter(self, image_data: bytes) -> bytes:
        # Placeholder for actual image processing
        return image_data

async def main():
    consumer = KafkaConsumer(topics=['raw-images'])
    producer = KafkaProducer(topic='processed-images')
    
    async with ParallelContext(max_workers=4) as context:
        processor = ImageProcessor()
        async for message in consumer:
            processed = await context.process(processor.process, message.value)
            await producer.send(processed)

async def main():
    logger = FogLogger("ImageProcessor")
    consumer = KafkaConsumer(
        topics=['raw-images'],
        bootstrap_servers="localhost:9092"
    )
    producer = KafkaProducer(
        topic='processed-images',
        bootstrap_servers="localhost:9092"
    )
    detector = AnomalyDetector(anomaly_threshold=100)
    
    await consumer.start()
    await producer.start()
    
    try:
        while True:
            message = await consumer.receive()
            # Monitor processing metrics
            metrics = {"messages_per_second": 150}  # Example metric
            if detector.detect_anomaly(metrics):
                logger.warning("Processing anomaly detected!")
            
            processor = ImageProcessor()
            processed = await processor.process(message)
            await producer.send(processed)
            logger.info("Processed and forwarded image")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
