# Spark Dockerfile
FROM bitnami/spark:3.5

# Install additional dependencies
RUN pip install --no-cache-dir \
    kafka-python \
    great-expectations \
    psycopg2-binary

# Copy Spark jobs
COPY data-ingestion/services/orchestration/spark/jobs/ /opt/spark/jobs/

# Set environment variables
ENV SPARK_DAEMON_JAVA_OPTS="-Dspark.driver.bindAddress=0.0.0.0" \
    PYTHONPATH=/opt/spark

# Default command
CMD ["spark-class", "org.apache.spark.deploy.master.Master"]
