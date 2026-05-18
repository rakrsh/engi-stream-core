# Ingestion Service Dockerfile
FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY data-ingestion/services/ingestion/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY data-ingestion/services/ingestion/src/ ./src/

# Create config directory
RUN mkdir -p /app/config

# Set Python path
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "-m", "src.app"]
