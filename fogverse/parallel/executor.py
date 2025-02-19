import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable

class ParallelExecutor:
    """Manages parallel task execution with process pool isolation."""
    
    def __init__(self, max_workers: int = None):
        self.pool = ProcessPoolExecutor(max_workers=max_workers)
        self.loop = asyncio.get_event_loop()

    async def run_in_process(self, 
                           func: Callable, 
                           *args,
                           callback: Callable = None) -> Any:
        """Executes function in separate process with optional callback."""
        result = await self.loop.run_in_executor(self.pool, func, *args)
        if callback:
            await self.loop.run_in_executor(None, callback, result)
        return result

    async def shutdown(self):
        """Gracefully terminates process pool."""
        self.pool.shutdown(wait=True)
