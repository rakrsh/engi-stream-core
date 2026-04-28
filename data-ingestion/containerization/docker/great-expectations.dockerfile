# Great Expectations Dockerfile
FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    GREAT_EXPECTATIONS_HOME=/tmp/gx

# Install dependencies
RUN pip install --no-cache-dir \
    great-expectations \
    pandas \
    pyspark \
    kafka-python

# Copy quality control files
COPY quality-control/ /tmp/gx/

# Set working directory
WORKDIR /tmp/gx

# Default command
CMD ["python", "-m", "great_expectations", "--help"]