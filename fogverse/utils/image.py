"""Manages all image-related operations."""

from fogverse.utils.data import bytes_to_numpy, numpy_to_bytes

import base64
import cv2

def _encode(img, encoding, *args):
    """Encodes an image using OpenCV."""

    _, encoded = cv2.imencode(f".{encoding}", img, *args)
    return encoded

def _decode(img):
    """Decodes an image from an encoded NumPy array."""

    return cv2.imdecode(img, cv2.IMREAD_COLOR)

def compress_encoding(img, encoding, *args):
    """Compresses an image by encoding it and converting it to bytes."""

    return numpy_to_bytes(_encode(img, encoding, *args))

def recover_encoding(img_bytes):
    """Recovers an image from its byte representation."""

    return _decode(bytes_to_numpy(img_bytes))

def numpy_to_base64_url(img, encoding, *args):
    """Converts a NumPy image array to a Base64-encoded data URL."""

    encoded_img = _encode(img, encoding, *args)
    b64 = base64.b64encode(encoded_img).decode()
    return f"data:image/{encoding};base64,{b64}"
