# Base image with Python and necessary dependencies.
FROM python:3.12.3

# Install system dependencies.
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    libsm6 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory.
WORKDIR /app

# Copy requirements first to leverage Docker cache.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the local fogverse repo into the container.
COPY ./fogverse /app/fogverse

# Install fogverse as an editable package.
RUN pip install -e /app/fogverse

# Copy the rest of the application.
COPY . .

# Environment variables.
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command.
CMD ["python", "manager.py"]
