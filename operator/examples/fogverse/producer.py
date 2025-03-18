import numpy as np
import asyncio

from fogverse import Producer
from dotenv import load_dotenv

load_dotenv()
class MyProducer(Producer):
    def __init__(self, loop=None):
        self.producer_topic = 'my-topic-1'
        Producer.__init__(self, loop=loop)

    async def receive(self):
        data = np.random.randint(100, size=(3, 3))
        return data

async def main():
    producer = MyProducer()
    tasks = [producer.run()]

    try:
        await asyncio.gather(*tasks)
    except:
        for t in tasks:
            t.close()

if __name__ == '__main__':
	asyncio.run(main())