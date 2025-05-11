# #!/bin/bash

# # Downloads the latest Apache Kafka binary, extracts it, renames the folder to 'kafka', and cleans up.

# show_spinner() {
#     local pid=$!
#     local delay=0.1
#     local spin='|/-\'
#     while kill -0 "$pid" 2>/dev/null; do
#         i=$(( (i + 1) % 4 ))
#         printf "\r%c" "${spin:$i:1}"
#         sleep $delay
#     done
#     printf "\r" # clear spinner after done.
# }

# set -e # Exit immediately if a command fails.

# echo "Determining the latest Kafka version."
# # This command fetches directory listing, extracts version-like strings, sorts them, and gets the latest.
# LATEST_VERSION=$(curl -s https://downloads.apache.org/kafka/ | \
#     grep -Eo '[0-9]+\.[0-9]+\.[0-9]+/' | \
#     sort -rV | head -n1 | tr -d '/')

# if [ -z "$LATEST_VERSION" ]; then
#     echo "Error: Could not determine the latest Kafka version. Check network or https://downloads.apache.org/kafka/."
#     exit 1
# fi
# echo "Latest Kafka version found: $LATEST_VERSION."

# # Assume Scala 2.13 for the binary; this is standard for recent Kafka releases.
# SCALA_BINARY_VERSION="2.13"
# KAFKA_ARCHIVE_NAME="kafka_${SCALA_BINARY_VERSION}-${LATEST_VERSION}.tgz"
# KAFKA_DOWNLOAD_URL="https://downloads.apache.org/kafka/${LATEST_VERSION}/${KAFKA_ARCHIVE_NAME}"

# echo "Downloading Kafka from $KAFKA_DOWNLOAD_URL."
# wget -q "$KAFKA_DOWNLOAD_URL" & # Downloads the archive quietly.
# show_spinner # Show spinner while downloading

# # Clean up old kafka folder before extracting.
# if [ -d "kafka" ]; then
#     echo "Removing existing 'kafka' directory."
#     rm -rf kafka
# fi

# echo "Extracting $KAFKA_ARCHIVE_NAME."
# tar -xzf "$KAFKA_ARCHIVE_NAME" # Extracts the archive.

# # The extracted folder name typically matches the archive name without the .tgz extension.
# EXTRACTED_FOLDER_NAME="${KAFKA_ARCHIVE_NAME%.tgz}"

# echo "Renaming $EXTRACTED_FOLDER_NAME to kafka."
# mv "$EXTRACTED_FOLDER_NAME" kafka # Renames the folder.

# echo "Removing the downloaded archive $KAFKA_ARCHIVE_NAME."
# rm "$KAFKA_ARCHIVE_NAME" # Deletes the .tgz file.

# echo "Kafka setup complete. Kafka is now in the 'kafka' directory."
# exit 0
