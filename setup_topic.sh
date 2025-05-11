# #!/bin/bash

# # delete topic, if exists.
# if [ ! -z "$(kafka/bin/kafka-topics.sh \
#               --list \
#               --topic test-topic \
#               --bootstrap-server localhost:29091)" ] ; then
#   echo "deleting topic"
#   kafka/bin/kafka-topics.sh \
#     --delete \
#     --topic test-topic \
#     --bootstrap-server localhost:29091
# fi

# # create topic.
# kafka/bin/kafka-topics.sh \
#   --create \
#   --topic test-topic \
#   --partitions 1 \
#   --replication-factor 1 \
#   --bootstrap-server localhost:29091
