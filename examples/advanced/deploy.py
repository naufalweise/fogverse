import os
from pathlib import Path
from dotenv import load_dotenv
from fogverse.utils.admin import configure_topics
from fogverse.utils.data import get_config
from manager import DeploymentManager
import asyncio

async def main():
    # Ensure Kafka topics exist.
    topics_config_path = Path(__file__).parent / "config" / "topics.yaml"
    configure_topics(topics_config_path, create=True)

    manager = DeploymentManager()
    await manager.start()
    await manager.deploy_pending_components()

if __name__ == '__main__':
    asyncio.run(main())
