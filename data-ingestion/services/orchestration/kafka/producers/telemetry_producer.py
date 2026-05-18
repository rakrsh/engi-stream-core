"""Kafka Producer for telemetry data.

Publishes real-time telemetry updates to Kafka topics.
"""

import asyncio
import json
from datetime import datetime
from typing import Any, Optional

from config.logger import get_logger
from config.settings import get_ingestion_settings
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = get_logger(__name__)


class TelemetryProducer:
    """Producer for publishing telemetry data to Kafka."""

    def __init__(self) -> None:
        self._settings = get_ingestion_settings()
        self._producer: Optional[KafkaProducer] = None

    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer."""
        return KafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            retry_backoff_ms=1000,
            max_in_flight_requests_per_connection=5,
            compression_type="gzip",
        )

    @property
    def producer(self) -> KafkaProducer:
        """Get or create the Kafka producer."""
        if self._producer is None:
            self._producer = self._create_producer()
        return self._producer

    def publish_wind_telemetry(
        self,
        wind_speed: float,
        wind_direction: float,
        temperature: float,
        pressure: float,
        location_lat: float,
        location_lon: float,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """Publish wind telemetry data.

        Args:
            wind_speed: Wind speed in m/s
            wind_direction: Wind direction in degrees
            temperature: Temperature in Celsius
            pressure: Pressure in Pa
            location_lat: Latitude
            location_lon: Longitude
            timestamp: Optional timestamp (defaults to now)

        Returns:
            True if successful, False otherwise
        """
        if timestamp is None:
            timestamp = datetime.now()

        message = {
            "type": "wind_telemetry",
            "timestamp": timestamp.isoformat(),
            "wind_speed": wind_speed,
            "wind_direction": wind_direction,
            "temperature": temperature,
            "pressure": pressure,
            "location": {"lat": location_lat, "lon": location_lon},
        }

        key = f"wind-{location_lat}-{location_lon}-{timestamp.timestamp()}"

        try:
            future = self.producer.send(
                self._settings.kafka_topic_telemetry, key=key, value=message
            )
            # Wait for send to complete
            future.get(timeout=10)
            logger.info(f"Published wind telemetry: speed={wind_speed}, dir={wind_direction}")
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish wind telemetry: {e}")
            return False

    def publish_atmospheric_telemetry(
        self,
        co2_concentration: Optional[float],
        surface_temperature: float,
        sea_level_pressure: float,
        wind_u: float,
        wind_v: float,
        location_lat: float,
        location_lon: float,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """Publish atmospheric telemetry data.

        Args:
            co2_concentration: CO2 concentration in ppm
            surface_temperature: Surface temperature in Kelvin
            sea_level_pressure: Sea level pressure in Pa
            wind_u: Zonal wind component in m/s
            wind_v: Meridional wind component in m/s
            location_lat: Latitude
            location_lon: Longitude
            timestamp: Optional timestamp (defaults to now)

        Returns:
            True if successful, False otherwise
        """
        if timestamp is None:
            timestamp = datetime.now()

        message = {
            "type": "atmospheric_telemetry",
            "timestamp": timestamp.isoformat(),
            "co2_concentration": co2_concentration,
            "surface_temperature": surface_temperature,
            "sea_level_pressure": sea_level_pressure,
            "wind_u": wind_u,
            "wind_v": wind_v,
            "location": {"lat": location_lat, "lon": location_lon},
        }

        key = f"atm-{location_lat}-{location_lon}-{timestamp.timestamp()}"

        try:
            future = self.producer.send(
                self._settings.kafka_topic_telemetry, key=key, value=message
            )
            future.get(timeout=10)
            logger.info(f"Published atmospheric telemetry: CO2={co2_concentration}")
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish atmospheric telemetry: {e}")
            return False

    def publish_batch_data(self, data: dict[str, Any], dataset_type: str = "wind") -> bool:
        """Publish batch data to Kafka.

        Args:
            data: The data to publish
            dataset_type: Type of dataset ("wind" or "satellite")

        Returns:
            True if successful, False otherwise
        """
        message = {
            "type": f"batch_{dataset_type}",
            "timestamp": datetime.now().isoformat(),
            "data": data,
        }

        key = f"batch-{dataset_type}-{datetime.now().timestamp()}"

        try:
            future = self.producer.send(self._settings.kafka_topic_batch, key=key, value=message)
            future.get(timeout=10)
            logger.info(f"Published batch data: type={dataset_type}")
            return True

        except KafkaError as e:
            logger.error(f"Failed to publish batch data: {e}")
            return False

    def close(self) -> None:
        """Close the Kafka producer."""
        if self._producer:
            self._producer.close()
            self._producer = None


# Convenience function for publishing
def publish_telemetry(
    wind_speed: Optional[float] = None,
    wind_direction: Optional[float] = None,
    temperature: Optional[float] = None,
    pressure: Optional[float] = None,
    co2_concentration: Optional[float] = None,
    surface_temperature: Optional[float] = None,
    sea_level_pressure: Optional[float] = None,
    wind_u: Optional[float] = None,
    wind_v: Optional[float] = None,
    location_lat: float = 0.0,
    location_lon: float = 0.0,
) -> bool:
    """Publish telemetry data to Kafka.

    This is a convenience function that handles both wind and atmospheric data.
    """
    producer = TelemetryProducer()

    try:
        success = True

        if wind_speed is not None and wind_direction is not None:
            success = (
                producer.publish_wind_telemetry(
                    wind_speed=wind_speed,
                    wind_direction=wind_direction,
                    temperature=temperature or 0.0,
                    pressure=pressure or 0.0,
                    location_lat=location_lat,
                    location_lon=location_lon,
                )
                and success
            )

        if co2_concentration is not None or surface_temperature is not None:
            success = (
                producer.publish_atmospheric_telemetry(
                    co2_concentration=co2_concentration,
                    surface_temperature=surface_temperature or 0.0,
                    sea_level_pressure=sea_level_pressure or 0.0,
                    wind_u=wind_u or 0.0,
                    wind_v=wind_v or 0.0,
                    location_lat=location_lat,
                    location_lon=location_lon,
                )
                and success
            )

        return success

    finally:
        producer.close()


if __name__ == "__main__":
    # Example usage
    publish_telemetry(
        wind_speed=12.5,
        wind_direction=180.0,
        temperature=25.0,
        pressure=101325.0,
        location_lat=39.7406,
        location_lon=-104.9917,
    )
