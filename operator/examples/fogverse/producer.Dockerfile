# Use an official Python runtime as base image
FROM python:3.13

# Set the working directory
WORKDIR /app

# Install Git and other required dependencies
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     git \
#     && rm -rf /var/lib/apt/lists/*

# Copy the application files
COPY producer.py /app
COPY requirements.txt /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

ENV PRODUCER_SERVERS=192.168.49.2:30912

# Run the application
CMD ["python", "producer.py"]
