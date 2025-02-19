import asyncio
from fogverse.admin import create_topics

async def main():
    kafka_config = {
        "bootstrap_servers": "localhost:9092",
        "topics": [
            {"name": "raw-images", "partitions": 6},
            {"name": "processed-images", "partitions": 6}
        ]
    }
    await create_topics(kafka_config)

if __name__ == "__main__":
    asyncio.run(main())
