from experiments.constants import JOLOKIA_AGENT_PATH, JOLOKIA_DOWNLOAD_URL, JOLOKIA_VERSION

def generate_jolokia_wrapper():
    # This script is used to create a wrapper for the Kafka entrypoint script
    # to ensure that the Jolokia agent is downloaded and configured correctly.
    # It checks for the presence of curl, installs it if necessary,
    # and then downloads the Jolokia agent if it is not already present.
    # The script also augments the KAFKA_OPTS environment variable to include
    # the Jolokia agent configuration.
    # Finally, it hands over to the original entrypoint script.
    script = f"""#!/bin/bash

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

JOLOKIA_AGENT_PATH={JOLOKIA_AGENT_PATH}
if [ ! -f $JOLOKIA_AGENT_PATH ]; then
  echo 'Node 0: Downloading Jolokia agent {JOLOKIA_VERSION}...'
  curl -L -s -o $JOLOKIA_AGENT_PATH \
{JOLOKIA_DOWNLOAD_URL}
  if [ $? -ne 0 ] || [ ! -s $JOLOKIA_AGENT_PATH ]; then
    echo 'Node 0: Failed to download Jolokia agent.' >&2
    exit 1
  fi
else
  echo 'Node 0: Jolokia agent already exists.'
fi

export KAFKA_OPTS="${{KAFKA_OPTS:-}} -javaagent:$JOLOKIA_AGENT_PATH=port=8778,host=0.0.0.0,discoveryEnabled=false"
echo "Node 0: Augmented KAFKA_OPTS: $KAFKA_OPTS"

# Finally hand over to the original entrypoint script.
exec /etc/confluent/docker/run "$@"
"""
    with open("jolokia-wrapper.sh", "w") as f:
        f.write(script)
