import asyncio
from fogverse.manager import FogManager
from fogverse.utils.data import get_config

async def main():
    fog_manager = FogManager(
        server=get_config("KAFKA_BOOTSTRAP_SERVERS"),
        topic=get_config("DEPLOYMENT_TOPIC"),
        yaml="examples/advanced/topics.yaml",
        to_deploy={
            "camera": {
                "image": "alexxit/go2rtc:latest",
                "env": {"CAMERA_INDEX": 0},
                "replicas": 1
            },
            # "processor": {
            #     "image": "codeproject/ai-server:latest",
            #     "env": {"MODEL_NAME": get_config("MODEL_NAME")},
            #     "replicas": get_config("NUM_INITIAL_WORKERS", cls=int)
            # },
        }
    )
    await fog_manager.run()

if __name__ == "__main__":
    asyncio.run(main())
