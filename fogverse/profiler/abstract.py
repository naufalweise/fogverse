from abc import ABC, abstractmethod

class AbstractProfiler(ABC):
    """Abstract base class for reliable profiling."""

    @abstractmethod
    def _before_receive(self, *args, **kwargs):
        """Hook to execute before receiving data."""
        pass

    @abstractmethod
    def _after_receive(self, *args, **kwargs):
        """Hook to execute after data has been received."""
        pass

    @abstractmethod
    def _before_decode(self, *args, **kwargs):
        """Hook to execute before decoding data."""
        pass

    @abstractmethod
    def _after_decode(self, *args, **kwargs):
        """Hook to execute after data has been decoded."""
        pass

    @abstractmethod
    def _before_process(self, *args, **kwargs):
        """Hook to execute before processing data."""
        pass

    @abstractmethod
    def _after_process(self, *args, **kwargs):
        """Hook to execute after data has been processed."""
        pass

    @abstractmethod
    def _before_encode(self, *args, **kwargs):
        """Hook to execute before encoding data."""
        pass

    @abstractmethod
    def _after_encode(self, *args, **kwargs):
        """Hook to execute after data has been encoded."""
        pass

    @abstractmethod
    def _before_send(self, *args, **kwargs):
        """Hook to execute before sending data."""
        pass

    @abstractmethod
    def _after_send(self, *args, **kwargs):
        """Hook to execute after data has been sent."""
        pass
