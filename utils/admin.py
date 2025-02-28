import os
import yaml

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, NewPartitions
from confluent_kafka.error import KafkaError
from constants import DEFAULT_NUM_PARTITIONS
from pathlib import Path

def resolve_server_env(server):
    """Resolve environment variables in server configurations."""

    resolved_server = server.copy()

    for server_name, host in resolved_server.items():
        if host.startswith('$'):  # Check if the host is an environment variable.
            env_var = host[1:]  # Remove the '$' prefix.
            host = os.getenv(env_var)  # Retrieve the value from environment variables.
            assert host is not None, f'Host {env_var} not found in environment variables.'

        resolved_server[server_name] = host  # Update the resolved server address.

    return resolved_server

def setup_topics(admin, topics, alter_if_exist=True):
    """Create or modify Kafka topics as needed."""
    
    if not topics:
        return  # No topics to process.

    # Ensure all topics are instances of NewTopic; convert lists/tuples to NewTopic if needed.
    topic_instances = [
        topic if isinstance(topic, NewTopic) else NewTopic(*topic) 
        for topic in topics
    ]

    responses = admin.create_topics(topic_instances)

    for topic in topic_instances:
        topic_name = topic.topic
        exc = responses[topic_name].exception(4)  # Wait up to 4 secs for a response.

        if exc and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            if alter_if_exist:
                print(f"Topic {topic_name} exists, updating...")
                increase_partitions(topic_name, admin, topic.num_partitions)
                alter_config(topic_name, admin, topic.config)
                status = "altered"
            else:
                print(f"Topic {topic_name} exists, skipping...")
                continue
        else:
            status = "created"

        print(f"Topic {topic_name} {status}.")

def increase_partitions(topic_name, admin, num_partitions):
    """
    Increase the number of partitions for a Kafka topic if requested_partitions is higher than the current count.

    If the requested number is lower than or equal to the current partitions, no change is made.
    """

    new_parts = NewPartitions(topic_name, num_partitions)
    responses = admin.create_partitions([new_parts])

    for future in responses.values():
        exc = future.exception(4)  # Wait up to 4 secs for response.
        if exc:
            error_code = exc.args[0].code() if exc.args else None
            if error_code == KafkaError.INVALID_PARTITIONS:
                print(f"Invalid partition update for topic {topic_name}. Skipping.")
                continue
            raise exc

def alter_config(topic_name, admin, config):
    """Update the configuration of an existing Kafka topic."""

    if not config:
        return # No config changes for topic.

    res = admin.alter_configs([
        ConfigResource(ConfigResource.Type.TOPIC, topic_name, set_config=config)
    ])

    for future in res.values():
        if (exc := future.exception(4)):  # Wait up to 4 secs for response.
            raise exc

def read_topic_yaml(filepath, env_var_resolver={}, create=False):
    """
    Read a YAML configuration file for Kafka topics and clusters, process topic settings,

    and optionally create the topics.
    """

    filepath = Path(filepath) if isinstance(filepath, str) else filepath

    with filepath.open() as f:
        config = yaml.safe_load(f)

    topic_config = config.get('topic', {})
    cluster_config = resolve_server_env(config.get('server', {}))

    # Initialize an AdminClient for each Kafka cluster.
    admins = {cluster: AdminClient({'bootstrap.servers': host})
              for cluster, host in cluster_config.items()}

    cluster_topics = {}
    # Process each topic and assign it to the appropriate Kafka clusters.
    for topic_name, attrs in topic_config.items():
        num_partitions = attrs.get('partitions', DEFAULT_NUM_PARTITIONS)
        topic_settings = attrs.get('config', {})
        target_clusters = attrs['server']
        formatted_topic = attrs.get('pattern', topic_name).format(**env_var_resolver)

        # Ensure target_clusters is always a list.
        if not isinstance(target_clusters, list):
            target_clusters = [target_clusters]

        # Group topics by cluster.
        for cluster in target_clusters:
            if cluster not in cluster_topics:
                cluster_topics[cluster] = {'admin': admins[cluster], 'topics': []}
            cluster_topics[cluster]['topics'].append((formatted_topic, num_partitions, topic_settings))

    if create:
        # Create topics on each Kafka cluster.
        for cluster_data in cluster_topics.values():
            setup_topics(cluster_data['admin'], cluster_data['topics'])

    return cluster_topics
