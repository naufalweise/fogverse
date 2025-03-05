# Advanced FogVerse Deployment

A production-grade video analytics pipeline with profiling, management, and auto-scaling.

## Components

1. **Smart Camera** - Captures and preprocesses video frames
2. **AI Processor** - Runs object detection models
3. **Results Analyzer** - Processes detection results
4. **Central Manager** - Orchestrates deployment
5. **Profiling Monitor** - Collects performance metrics

## Features

- Real-time performance profiling
- Dynamic component deployment
- Kafka-based messaging
- Docker containerization
- Automated scaling decisions

## Running

1. Start infrastructure:

```bash
docker-compose up -d --build
```

2. Deploy components:

```bash
python deploy.py
```

3. Monitor profiling:

```bash
python monitor.py
```

4. Scale workers:

```bash
python scale_workers.py
```

5. Clean up:

```bash
docker-compose down -v
```
