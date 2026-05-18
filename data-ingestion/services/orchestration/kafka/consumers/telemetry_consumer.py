"""Kafka Consumer for telemetry and batch data.

Consumes messages from Kafka topics and processes them.
"""

import json
from datetime import datetime
from typing import Any, Callable, Optional

from config.logger import get_logger
from config.settings import get_ingestion_settings
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = get_logger(__name__)


class TelemetryConsumer:
    """Consumer for reading telemetry data from Kafka."""

    def __init__(self, topics: Optional[list[str]] = None, group_id: Optional[str] = None) -> None:
        """Initialize the telemetry consumer.

        Args:
            topics: List of Kafka topics to subscribe to
            group_id: Kafka consumer group ID
        """
        self._settings = get_ingestion_settings()
        self._topics = topics or [self._settings.kafka_topic_telemetry]
        self._group_id = group_id or self._settings.kafka_consumer_group
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False

    def _create_consumer(self) -> KafkaConsumer:
        """Create and configure Kafka consumer."""
        return KafkaConsumer(
            *self._topics,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )

    @property
    def consumer(self) -> KafkaConsumer:
        """Get or create the Kafka consumer."""
        if self._consumer is None:
            self._consumer = self._create_consumer()
        return self._consumer

    def consume_messages(
        self,
        callback: Callable[[dict[str, Any]], None],
        max_messages: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> int:
        """Consume messages from Kafka topics.

        Args:
            callback: Function to process each message
            max_messages: Maximum number of messages to process (None for unlimited)
            timeout_seconds: Timeout in seconds (None for no timeout)

        Returns:
            Number of messages processed
        """
        self._running = True
        processed = 0
        start_time = datetime.now()

        try:
            for message in self.consumer:
                if not self._running:
                    break

                try:
                    callback(message.value)
                    processed += 1
                    logger.debug(f"Processed message {processed}: {message.value.get('type')}")

                    if max_messages and processed >= max_messages:
                        logger.info(f"Reached max messages limit: {max_messages}")
                        break

                    if timeout_seconds:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if elapsed >= timeout_seconds:
                            logger.info(f"Reached timeout: {timeout_seconds}s")
                            break

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")

        finally:
            logger.info(f"Consumer stopped. Processed {processed} messages")

        return processed

    def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False

    def close(self) -> None:
        """Close the Kafka consumer."""
        self._running = False
        if self._consumer:
            self._consumer.close()
            self._consumer = None


class BatchConsumer:
    """Consumer for reading batch data from Kafka."""

    def __init__(self, topics: Optional[list[str]] = None, group_id: Optional[str] = None) -> None:
        """Initialize the batch consumer.

        Args:
            topics: List of Kafka topics to subscribe to
            group_id: Kafka consumer group ID
        """
        self._settings = get_ingestion_settings()
        self._topics = topics or [self._settings.kafka_topic_batch]
        self._group_id = group_id or f"{self._settings.kafka_consumer_group}-batch"
        self._consumer: Optional[KafkaConsumer] = None

    def _create_consumer(self) -> KafkaConsumer:
        """Create and configure Kafka consumer."""
        return KafkaConsumer(
            *self._topics,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

    @property
    def consumer(self) -> KafkaConsumer:
        """Get or create the Kafka consumer."""
        if self._consumer is None:
            self._consumer = self._create_consumer()
        return self._consumer

    def consume_batch(
        self, callback: Callable[[dict[str, Any]], None], batch_size: int = 100
    ) -> list[dict[str, Any]]:
        """Consume a batch of messages from Kafka.

        Args:
            callback: Function to process each message
            batch_size: Number of messages to consume in one batch

        Returns:
            List of processed messages
        """
        processed = []

        try:
            messages = []
            for message in self.consumer:
                messages.append(message.value)
                if len(messages) >= batch_size:
                    break

            for msg in messages:
                try:
                    callback(msg)
                    processed.append(msg)
                except Exception as e:
                    logger.error(f"Error processing batch message: {e}")

        except KafkaError as e:
            logger.error(f"Kafka batch consumer error: {e}")

        logger.info(f"Batch consumer processed {len(processed)} messages")
        return processed

    def close(self) -> None:
        """Close the Kafka consumer."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None


# Example callback functions
def process_wind_telemetry(message: dict[str, Any]) -> None:
    """Process wind telemetry message."""
    logger.info(
        f"Wind telemetry: speed={message.get('wind_speed')}, "
        f"dir={message.get('wind_direction')}, "
        f"temp={message.get('temperature')}"
    )


def process_atmospheric_telemetry(message: dict[str, Any]) -> None:
    """Process atmospheric telemetry message."""
    logger.info(
        f"Atmospheric telemetry: CO2={message.get('co2_concentration')}, "
        f"temp={message.get('surface_temperature')}, "
        f"pressure={message.get('sea_level_pressure')}"
    )


def process_batch_data(message: dict[str, Any]) -> None:
    """Process batch data message."""
    logger.info(f"Batch data: type={message.get('type')}")


if __name__ == "__main__":
    # Example: Consume telemetry messages
    consumer = TelemetryConsumer()

    try:
        print("Starting telemetry consumer...")
        consumer.consume_messages(callback=process_wind_telemetry, timeout_seconds=60)
    finally:
        consumer.close()
