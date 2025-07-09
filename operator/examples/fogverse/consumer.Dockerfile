# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install git (required for installing fogverse from Git)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy the application files
COPY consumer.py /app
COPY requirements.txt /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the application
CMD ["python", "consumer.py"]
