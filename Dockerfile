# ============================================================
# Producer image — Python 3.11 slim
# ============================================================
# Uses Python 3.11 which has stable pre-built wheels for all
# data engineering packages (confluent-kafka, PySpark, etc.)
# No C++ build tools needed — wheels install directly.
# ============================================================

FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Add /app to PYTHONPATH so that `from config.settings import ...`
# and `from producer.ride_event_generator import ...` resolve correctly
ENV PYTHONPATH=/app

# Install system dependencies needed by confluent-kafka's librdkafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first — Docker caches this layer separately.
# As long as requirements.txt doesn't change, pip install is skipped on rebuild.
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY config/ ./config/
COPY producer/ ./producer/

# Default command — can be overridden in docker-compose or at runtime
# Runs the producer in continuous mode at the configured rate
CMD ["python", "producer/kafka_producer.py"]
