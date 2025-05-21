#!/bin/bash

echo "Node 0: Checking for curl..."
if ! command -v curl &> /dev/null; then
  echo 'Node 0: curl not found. Attempting to install...'
  if command -v microdnf &> /dev/null; then
    microdnf install -y curl
  elif command -v apt-get &> /dev/null; then
    apt-get update && apt-get install -y curl
  else
    echo 'Node 0: Cannot install curl. Please ensure curl is in the base image.' >&2
    exit 1
  fi
fi

JOLOKIA_AGENT_PATH=/tmp/jolokia-jvm-2.2.9-agent.jar
if [ ! -f $JOLOKIA_AGENT_PATH ]; then
  echo 'Node 0: Downloading Jolokia agent 2.2.9...'
  curl -L -s -o $JOLOKIA_AGENT_PATH https://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-agent-jvm/2.2.9/jolokia-agent-jvm-2.2.9-javaagent.jar
  if [ $? -ne 0 ] || [ ! -s $JOLOKIA_AGENT_PATH ]; then
    echo 'Node 0: Failed to download Jolokia agent.' >&2
    exit 1
  fi
else
  echo 'Node 0: Jolokia agent already exists.'
fi

export KAFKA_OPTS="${KAFKA_OPTS:-} -javaagent:$JOLOKIA_AGENT_PATH=port=8778,host=0.0.0.0,discoveryEnabled=false"
echo "Node 0: Augmented KAFKA_OPTS: $KAFKA_OPTS"

# Finally hand over to the original entrypoint script.
exec /etc/confluent/docker/run "$@"
