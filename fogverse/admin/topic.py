from confluent_kafka.admin import AdminClient, NewTopic

async def create_topics(config: dict):
    """Create Kafka topics with specified configurations."""
    admin_client = AdminClient({'bootstrap.servers': config['bootstrap_servers']})
    
    # Create NewTopic objects with proper configuration.
    topics = [
        NewTopic(
            topic['name'],
            num_partitions=topic.get('partitions', 1),
            replication_factor=topic.get('replication_factor', 1),
            config={
                'retention.ms': '30000',
                'segment.bytes': '1000000',
                **topic.get('config', {})
            }
        ) for topic in config.get('topics', [])
    ]
    
    # Create topics with timeout.
    futures = admin_client.create_topics(topics, operation_timeout=30)
    
    for topic, future in futures.items():
        try:
            future.result()  # Wait for topic creation.
            print(f"Successfully created topic: {topic}")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
