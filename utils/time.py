"""Manages all datetime-related operations."""

from datetime import datetime

# Default format for timestamps.
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

def get_timestamp(utc=True):
    """Returns the current timestamp, either UTC or local."""

    return datetime.utcnow() if utc else datetime.now()

def get_timestamp_str(date=None, utc=True, format=DATETIME_FORMAT):
    """Formats a datetime object as a string."""

    date = date or get_timestamp(utc)
    return date.strftime(format)

def timestamp_to_datetime(timestamp, format=DATETIME_FORMAT):
    """Converts a timestamp string into a datetime object."""

    if isinstance(timestamp, bytes):
        timestamp = timestamp.decode()
    return datetime.strptime(timestamp, format)

def calc_datetime(start, end=None, format=DATETIME_FORMAT, decimals=2, utc=True):
    """Computes the time difference in milliseconds between two timestamps."""

    if start is None:
        return -1  # Indicates missing start time.
    
    if isinstance(start, str):
        start = datetime.strptime(start, format)

    if end is None:
        end = get_timestamp(utc)
    elif isinstance(end, str):
        end = datetime.strptime(end, format)

    diff = (end - start).total_seconds() * 1e3
    return round(diff, decimals)
