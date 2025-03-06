from dataclasses import dataclass
from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions, NewTopic
from confluent_kafka.error import KafkaError
from fogverse.constants import DEFAULT_NUM_PARTITIONS
from fogverse.utils.data import resolve_env_variables
from pathlib import Path

import yaml

def configure_topics(filepath: str, create=False):
    """
    Retrieves Kafka topics and cluster configurations from a YAML file,
    substituting any environment variables on the fly,
    and optionally creating topics when needed.
    """

    # Open and parse the YAML file.
    with Path(filepath).open() as f:
        config = yaml.safe_load(f) or {}

    # Resolve environment variables in topic and server configurations.
    topic_config = resolve_env_variables(config.get("topic", {}))
    cluster_config = resolve_env_variables(config.get("server", {}))

    # Initialize a dictionary to manage cluster connections and topic lists.
    cluster_topics = {
        host: {"admin": AdminClient({"bootstrap.servers": host}), "topics": []} for host in cluster_config.values()
    }

    # This servers as a Data Transfer Object.
    @dataclass
    class TopicConfig:
        name: str
        num_partitions: int
        config: dict

    # Extract and assign topics to their respective clusters.
    for topic_name, attrs in topic_config.items():
        assigned_clusters = attrs.get("server", "localhost")
        num_partitions = attrs.get("partitions", DEFAULT_NUM_PARTITIONS)
        topic_extra_config = attrs.get("config", {})

        # Normalize `assigned_clusters` to a list.
        assigned_clusters = assigned_clusters if isinstance(assigned_clusters, list) else [assigned_clusters]

        for cluster in assigned_clusters:
            cluster_topics[cluster]["topics"].append(TopicConfig(topic_name, num_partitions, topic_extra_config))

    if create:
        for cluster_info in cluster_topics.values():
            setup_topics(cluster_info["admin"], cluster_info["topics"])

    return cluster_topics

def setup_topics(admin, topics, alter_if_exist=True):
    """Create or update Kafka topics as needed."""

    if not topics:
        return  # No topics to process.

    # Ensure topics are NewTopic instances.
    topic_instances = [
        topic if isinstance(topic, NewTopic) else NewTopic(topic.name, num_partitions=topic.num_partitions, config=topic.config) 
        for topic in topics
    ]

    responses = admin.create_topics(topic_instances)

    for topic in topic_instances:
        topic_name, num_partitions, config = topic.topic, topic.num_partitions, topic.config
        error = responses[topic_name].exception(4)  # Wait up to 4 secs.

        if not error:
            print(f"Topic {topic_name} created.")
            continue

        if error.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            if alter_if_exist:
                print(f"Topic {topic_name} exists, updating...")
                increase_partitions(topic_name, admin, num_partitions)
                alter_config(topic_name, admin, config)
                print(f"Topic {topic_name} altered.")
            else:
                print(f"Topic {topic_name} exists, skipping...")

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
            if error_code == KafkaError.INVALID_PARTITIONS:  # Topic already has more partitions or invalid `num_partitions`.
                print(f"Invalid partition update for topic {topic_name}. Skipping.")
                continue
            raise exc

def alter_config(topic_name, admin, config):
    """Update the configuration of an existing Kafka topic."""

    if not config:
        return  # No config changes for topic.

    res = admin.alter_configs([
        ConfigResource(ConfigResource.Type.TOPIC, topic_name, set_config=config)
    ])

    for future in res.values():
        if (exc := future.exception(4)):  # Wait up to 4 secs for response.
            raise exc
