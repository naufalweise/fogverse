from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions, NewTopic
from confluent_kafka.error import KafkaError
from fogverse.constants import DEFAULT_NUM_PARTITIONS
from pathlib import Path

import os
import yaml

def configure_topics(filepath: str, create=False):
    """
    Retrieves Kafka topics and cluster configurations from a YAML file,
    substituting any environment variables on the fly,
    and optionally creating topics when needed.
    """

    with Path(filepath).open() as f:
        config = yaml.safe_load(f) or {}

    topic_config = resolve_env_variables(config.get("topic", {}))
    cluster_config = resolve_env_variables(config.get("server", {}))

    admins = {cluster: AdminClient({"bootstrap.servers": host}) for cluster, host in cluster_config.items()}
    cluster_topics = {cluster: {"admin": admins[cluster], "topics": []} for cluster in cluster_config}

    for topic_name, attrs in topic_config.items():
        assigned_clusters = attrs.get("server", "localhost")
        num_partitions = attrs.get("partitions", DEFAULT_NUM_PARTITIONS)
        topic_properties = attrs.get("config", {})

        # Normalize `assigned_clusters` to a list.
        assigned_clusters = assigned_clusters if isinstance(assigned_clusters, list) else [assigned_clusters]

        for cluster in assigned_clusters:
            cluster_topics[cluster]["topics"].append((topic_name, num_partitions, topic_properties))

    if create:
        for cluster_info in cluster_topics.values():
            setup_topics(cluster_info["admin"], cluster_info["topics"])

    return cluster_topics

def resolve_env_variables(mapping: dict):
    """
    This function scans the dictionary for values that start with "$",
    treats them as environment variable references, and replaces them
    with their actual values from the system"s environment variables.
    If an environment variable is not found, an assertion error is raised.
    """

    result = {}

    for key, value in mapping.items():
        if isinstance(value, str) and value.startswith("$"):  # Check if the value is an env variable.
            env_var = value[1:]  # Remove the "$" prefix.
            resolved = os.getenv(env_var)  # Retrieve the value from environment variables.
            assert resolved is not None, f"Environment variable {env_var} not found."

        result[key] = resolved

    return result

def setup_topics(admin, topics, alter_if_exist=True):
    """Create or update Kafka topics as needed."""

    if not topics:
        return  # No topics to process.

    # Ensure topics are NewTopic instances.
    topic_instances = [
        topic if isinstance(topic, NewTopic) else NewTopic(*topic) 
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
            if error_code == KafkaError.INVALID_PARTITIONS:
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
