# Airflow Dockerfile
FROM apache/airflow:2.8.1

# Install additional dependencies
RUN pip install --no-cache-dir \
    kafka-python \
    great-expectations \
    psycopg2-binary \
    aiohttp

# Copy DAGs
COPY data-ingestion/services/orchestration/airflow/dags/ /opt/airflow/dags/

# Copy Spark jobs
COPY data-ingestion/services/orchestration/spark/jobs/ /opt/spark/jobs/

# Set environment variables
ENV PYTHONPATH=/opt/airflow:/opt/spark

# Default arguments
CMD ["webserver"]
