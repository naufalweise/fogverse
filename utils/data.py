"""Manages all data and configuration operations."""

import inspect
import numpy as np
import os
import sys
import uuid

from io import BytesIO

def get_cam_id():
    """Generates a camera ID using an environment variable if available, otherwise creates a UUID."""

    return f"cam_{os.getenv('CAM_ID', str(uuid.uuid4()))}"

def bytes_to_numpy(bbytes):
    """Converts byte data into a NumPy array."""

    with BytesIO(bbytes) as f:
        return np.load(f, allow_pickle=True)

def numpy_to_bytes(arr):
    """Converts a NumPy array into byte data."""

    with BytesIO() as f:
        np.save(f, arr)
        return f.getvalue()

def get_size(obj, seen=None):
    """
    Recursively calculates the memory size of an object in bytes.
    Adapted from https://github.com/bosswissam/pysize/blob/master/pysize.py.
    """

    size = sys.getsizeof(obj)  # Base size of the object.
    
    if seen is None:
        seen = set()
    
    obj_id = id(obj)
    if obj_id in seen:
        return 0  # Prevents infinite recursion in self-referential objects.
    
    seen.add(obj_id)

    # Recursively calculate the size of object attributes.
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if inspect.isgetsetdescriptor(d) or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break

    # Calculate size of dictionary keys and values.
    if isinstance(obj, dict):
        size += sum(get_size(v, seen) for v in obj.values())
        size += sum(get_size(k, seen) for k in obj.keys())

    # Calculate size of iterable elements.
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        try:
            size += sum(get_size(i, seen) for i in obj)
        except TypeError:
            pass

    # Calculate size of __slots__ attributes.
    if hasattr(obj, '__slots__'):
        size += sum(get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s))

    return size

def size_kb(obj, decimals=3):
    """Converts an object's memory size to kilobytes with a specified decimal precision."""

    return round(get_size(obj) / 1e3, decimals)

def get_header(headers, key, default=None, decoder=None):
    """Retrieves a header value from a list of headers, with optional decoding."""

    if not headers or key is None:
        return default
    
    for header in headers:
        if header[0] == key:
            val = header[1]
            if callable(decoder):
                return decoder(val)
            if isinstance(val, bytes):
                return val.decode()
            return val
    
    return default

# Placeholder class for a null object.
class _Null:
    pass

_null = _Null()

def get_config(config_name: str, cls: object = None, default=None):
    """Retrieves a configuration value from environment variables or a class attribute."""

    ret = os.getenv(config_name, _null)
    
    if not isinstance(ret, _Null):
        return ret
    
    if cls is None:
        return ret or default
    
    return getattr(cls, config_name.lower(), default)
