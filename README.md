# FogVerse Framework

A distributed computing framework for building fog computing applications with Kafka message queues, video processing capabilities, and performance profiling.

## Core Components

### Runnable Base Class

The [`Runnable`](fogverse/runnable.py) class serves as the foundation for all processing components, implementing a pipeline with:

- Message receiving
- Decoding
- Processing
- Encoding
- Sending
- Lifecycle hooks for profiling and monitoring

### Consumers

- [`KafkaConsumer`](fogverse/consumer/kafka.py) - Consumes messages from Kafka topics
- [`ConsumerOpenCV`](fogverse/consumer/open_cv.py) - Reads video frames from cameras/files
- [`ConsumerStorage`](fogverse/consumer/storage.py) - Stores messages in an async queue

### Producers

- [`KafkaProducer`](fogverse/producer/kafka.py) - Publishes messages to Kafka topics

### Profiling

- [`FogProfiler`](fogverse/profiler/fog.py) - Records performance metrics:
  - Frame processing delays
  - Message sizes
  - Processing times
  - Encode/decode times
  - Memory usage

### Logging

- [`FogLogger`](fogverse/logger/fog.py) - Unified logging with:
  - Console output
  - Text file logging
  - CSV metrics logging
  - Rotating file handlers

### Utils

- [`admin.py`](fogverse/utils/admin.py) - Kafka topic management
- [`data.py`](fogverse/utils/data.py) - Data conversion and env var handling
- [`image.py`](fogverse/utils/image.py) - Image encoding/decoding
- [`logger.py`](fogverse/utils/logger.py) - Logger configuration
- [`time.py`](fogverse/utils/time.py) - Timestamp utilities
