import io
import numpy as np
import asyncio

from fogverse import Consumer
from dotenv import load_dotenv

load_dotenv()
class MyConsumer(Consumer):
	def __init__(self, loop=None):
		self.consumer_topic = 'my-topic-1'
		Consumer.__init__(self)

	async def _send(self, data, *args, **kwargs):
		def __send(data):
			array = np.load(io.BytesIO(data))
			print (array)
		return await self._loop.run_in_executor(None, __send, data)

async def main():
	consumer = MyConsumer()
	tasks = [consumer.run()]

	try:
		await asyncio.gather(*tasks)
	except:
		for t in tasks:
			t.close()

if __name__ == '__main__':
	asyncio.run(main())