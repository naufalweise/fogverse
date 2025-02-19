import abc
import asyncio

class BaseProducer(abc.ABC):
    """Abstract base class for all producers. Implements core production flow."""
    
    def __init__(self, auto_encode: bool = True):
        self.auto_encode = auto_encode
        self._running = False
        
    async def start(self):
        """Initialize producer connections."""
        self._running = True
        
    async def stop(self):
        """Gracefully shutdown producer."""
        self._running = False
        
    @abc.abstractmethod
    async def _send(self, data: bytes):
        """Implement low-level message sending."""
        
    async def send(self, data):
        """Send data with optional encoding."""
        if self.auto_encode:
            data = self.encode(data)
        await self._send(data)
        
    def encode(self, data) -> bytes:
        """Convert data to bytes. Override for custom encoding."""
        if isinstance(data, bytes):
            return data
        return str(data).encode('utf-8')
