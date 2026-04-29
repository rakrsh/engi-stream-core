# Kafka Dockerfile
FROM confluentinc/cp-kafka:7.5.0

# Install additional tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Copy configuration
COPY containerization/docker/kafka/server.properties /etc/kafka/server.properties
COPY containerization/docker/kafka/zookeeper.properties /etc/kafka/zookeeper.properties

# Expose ports
EXPOSE 9092 29092 8081

# Default command
CMD ["start-kafka.sh"]
