import json
from typing import Any

def serialize(data: Any) -> bytes:
    """Serialize data to bytes using JSON."""
    return json.dumps(data).encode('utf-8')

def deserialize(data: bytes) -> Any:
    """Deserialize bytes to Python object using JSON."""
    return json.loads(data.decode('utf-8'))
