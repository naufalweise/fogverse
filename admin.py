import os
import yaml

from confluent_kafka import admin, KafkaError
from pathlib import Path

def check_server_env(server):
    """Resolve environment variables in server configurations."""

    resolved_server = server.copy()

    for server_name, host in resolved_server.items():
        if host.startswith('$'):  # Check if the host is an environment variable.
            env_var = host[1:]  # Remove the '$' prefix.
            host = os.getenv(env_var)  # Retrieve the value from environment variables.
            assert host is not None, f'Host {env_var} not found in environment variables.'

        resolved_server[server_name] = host  # Update the resolved server address.

    return resolved_server

def create_topics(client: admin.AdminClient, topics, alter_if_exist=True):
    """Create Kafka topics, with optional alterations if they already exist."""

    if not topics:
        return  # Exit early if no topics are provided.

    topic_instances = []

    # Convert topic definitions into NewTopic instances if needed.
    for topic in topics:
        if isinstance(topic, admin.NewTopic):
            topic_instances.append(topic)
        else:
            topic_name, num_partitions, config = topic
            topic_instances.append(admin.NewTopic(topic_name, num_partitions, config))

    # Request Kafka to create topics.
    res = client.create_topics(topic_instances)

    for topic_instance in topic_instances:
        topic_name = topic_instance.topic
        exc = res[topic_name].exception(5)  # Wait up to 5 seconds for response.

        if not alter_if_exist:
            print(f'Topic {topic_name} already exists, skipping...')
            continue

        if exc is not None:
            err = exc.args[0]
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f'Topic {topic_name} already exists, altering configs...')
                create_partitions(topic_name, client, topic_instance.num_partitions)
                alter_config(topic_name, client, topic_instance.config)
                status = 'altered'
        else:
            status = 'created'

        print(f'Topic {topic_name} has been {status}.')

def alter_config(topic_name, adm, config):
    """Modify the configuration of an existing Kafka topic."""

    if not config:
        return  # Exit if there is no configuration to update.

    res = adm.alter_configs([
        admin.ConfigResource(admin.ConfigResource.Type.TOPIC, topic_name, set_config=config)
    ])

    # Check for exceptions in the alteration process.
    for future in res.values():
        exc = future.exception(5)  # Wait up to 5 seconds for response.
        if exc is not None:
            raise exc

def create_partitions(topic_name, adm, num_partitions):
    """Increase the number of partitions in a Kafka topic if needed."""

    new_partitions = admin.NewPartitions(topic_name, num_partitions)
    res = adm.create_partitions([new_partitions])

    # Check for exceptions and handle errors.
    for future in res.values():
        exc = future.exception(5)  # Wait up to 5 seconds for response.
        if exc is not None:
            err = exc.args[0]
            if err.code() == KafkaError.INVALID_PARTITIONS:
                continue  # Ignore invalid partition errors.
            raise exc

def read_topic_yaml(filepath, str_format={}, create=False):
    """Read a YAML file, process topic configurations, and optionally create Kafka topics."""

    filepath = Path(filepath) if isinstance(filepath, str) else filepath

    with filepath.open() as f:
        config = yaml.safe_load(f)  # Load the YAML configuration.

    topic_config = config['topic']
    server_config = check_server_env(config['server'])  # Resolve environment variables.

    final_data = {}  # Store topic configurations grouped by server.
    admins = {}

    # Initialize Kafka admin clients for each server.
    for server_name, host in server_config.items():
        admins[server_name] = admin.AdminClient({'bootstrap.servers': host})

    # Process topic configurations.
    for topic_name, attr in topic_config.items():
        num_partitions = attr.get('partitions', 1)
        topic_settings = attr.get('config', {})
        assigned_servers = attr['server']

        # Apply string formatting if a pattern is provided.
        formatted_topic_name = attr.get('pattern', topic_name).format(**str_format)

        # Ensure assigned_servers is a list.
        assigned_servers = [assigned_servers] if not isinstance(assigned_servers, list) else assigned_servers

        # Assign topics to respective servers.
        for server_name in assigned_servers:
            adm = admins[server_name]
            host = server_config[server_name]
            
            final_data.setdefault(host, {'admin': adm, 'topics': []})
            final_data[host]['topics'].append((formatted_topic_name, num_partitions, topic_settings))

    if not create:
        return final_data  # Return processed data if not creating topics.

    # Create topics on each Kafka server.
    for topic_data in final_data.values():
        _admin = topic_data['admin']
        topics = topic_data['topics']
        create_topics(_admin, topics)

    print('Done.')
