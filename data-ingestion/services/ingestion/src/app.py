"""Main Ingestion Service Entry Point.

Provides both batch and real-time ingestion capabilities.
"""
import asyncio
import json
from datetime import datetime
from typing import Any, Optional

from config.logger import get_logger
from config.settings import get_ingestion_settings
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from modules.nasa_fetcher import NASASatelliteFetcher, SatelliteDataset
from modules.nrel_fetcher import NRELWindFetcher, WindDataset

logger = get_logger(__name__)


class DataIngestionService:
    """Main service for ingesting NREL and NASA data."""

    def __init__(self) -> None:
        self._settings = get_ingestion_settings()
        self._kafka_producer: Optional[KafkaProducer] = None
        self._nrel_fetcher = NRELWindFetcher()
        self._nasa_fetcher = NASASatelliteFetcher()

    async def _init_kafka_producer(self) -> KafkaProducer:
        """Initialize Kafka producer."""
        if self._kafka_producer is None:
            self._kafka_producer = KafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
        return self._kafka_producer

    async def close(self) -> None:
        """Close all connections."""
        await self._nrel_fetcher.close()
        await self._nasa_fetcher.close()
        if self._kafka_producer:
            self._kafka_producer.close()

    # ==================== Batch Ingestion ====================

    async def ingest_wind_data_batch(
        self, lat: float, lon: float, hub_height: float = 80.0
    ) -> WindDataset:
        """Ingest wind data in batch mode.

        Fetches data and sends to Kafka batch topic.
        """
        logger.info(f"Starting batch ingestion for wind data at ({lat}, {lon})")

        # Fetch data from NREL
        dataset = await self._nrel_fetcher.fetch_wind_data(
            lat, lon, hub_height=hub_height
        )

        # Send to Kafka batch topic
        producer = await self._init_kafka_producer()
        for data_point in dataset.data_points:
            message = {
                "dataset_id": dataset.dataset_id,
                "location_name": dataset.location_name,
                "timestamp": data_point.timestamp.isoformat(),
                "wind_speed": data_point.wind_speed,
                "wind_direction": data_point.wind_direction,
                "wind_direction_cardinal": data_point.wind_direction_cardinal,
                "turbulence_intensity": data_point.turbulence_intensity,
                "air_density": data_point.air_density,
                "temperature": data_point.temperature,
                "pressure": data_point.pressure,
                "hub_height": data_point.hub_height,
                "location_lat": data_point.location_lat,
                "location_lon": data_point.location_lon,
                "fetched_at": dataset.fetched_at.isoformat(),
            }

            producer.send(
                self._settings.kafka_topic_batch, key=f"{lat},{lon}", value=message
            )

        producer.flush()
        logger.info(
            f"Batch ingestion complete: {len(dataset.data_points)} points sent to Kafka"
        )

        return dataset

    async def ingest_satellite_data_batch(
        self, lat: float, lon: float, date: Optional[datetime] = None
    ) -> SatelliteDataset:
        """Ingest satellite data in batch mode.

        Fetches data and sends to Kafka batch topic.
        """
        logger.info(f"Starting batch ingestion for satellite data at ({lat}, {lon})")

        # Fetch data from NASA
        dataset = await self._nasa_fetcher.fetch_earth_imagery(lat, lon, date)

        # Send to Kafka batch topic
        producer = await self._init_kafka_producer()

        # Send images
        for image in dataset.images:
            message = {
                "dataset_id": dataset.dataset_id,
                "satellite_name": dataset.satellite_name,
                "image_id": image.image_id,
                "capture_date": image.capture_date.isoformat(),
                "cloud_cover": image.cloud_cover,
                "location_lat": image.location_lat,
                "location_lon": image.location_lon,
                "image_url": image.image_url,
                "source": image.source,
                "fetched_at": dataset.fetched_at.isoformat(),
            }

            producer.send(
                self._settings.kafka_topic_batch, key=image.image_id, value=message
            )

        # Send atmospheric data
        for atm_data in dataset.atmospheric_data:
            message = {
                "dataset_id": dataset.dataset_id,
                "satellite_name": dataset.satellite_name,
                "timestamp": atm_data.timestamp.isoformat(),
                "co2_concentration": atm_data.co2_concentration,
                "ch4_concentration": atm_data.ch4_concentration,
                "ozone_concentration": atm_data.ozone_concentration,
                "aerosol_optical_depth": atm_data.aerosol_optical_depth,
                "surface_temperature": atm_data.surface_temperature,
                "sea_level_pressure": atm_data.sea_level_pressure,
                "wind_u": atm_data.wind_u,
                "wind_v": atm_data.wind_v,
                "location_lat": atm_data.location_lat,
                "location_lon": atm_data.location_lon,
                "fetched_at": dataset.fetched_at.isoformat(),
            }

            producer.send(
                self._settings.kafka_topic_batch,
                key=f"atm-{atm_data.timestamp.isoformat()}",
                value=message,
            )

        producer.flush()
        logger.info(
            f"Batch ingestion complete: {len(dataset.images)} images, {len(dataset.atmospheric_data)} atmospheric points"
        )

        return dataset

    # ==================== Real-time Ingestion ====================

    async def ingest_telemetry_realtime(
        self,
        lat: float,
        lon: float,
        interval_seconds: int = 60,
        duration_minutes: int = 60,
    ) -> None:
        """Ingest telemetry data in real-time mode.

        Continuously fetches data and sends to Kafka telemetry topic.
        """
        logger.info(f"Starting real-time telemetry ingestion at ({lat}, {lon})")

        producer = await self._init_kafka_producer()
        end_time = datetime.now().timestamp() + (duration_minutes * 60)

        while datetime.now().timestamp() < end_time:
            # Fetch current wind data
            dataset = await self._nrel_fetcher.fetch_wind_data(lat, lon)

            if dataset.data_points:
                latest = dataset.data_points[-1]
                message = {
                    "type": "telemetry",
                    "source": "nrel",
                    "timestamp": latest.timestamp.isoformat(),
                    "wind_speed": latest.wind_speed,
                    "wind_direction": latest.wind_direction,
                    "temperature": latest.temperature,
                    "pressure": latest.pressure,
                    "location_lat": latest.location_lat,
                    "location_lon": latest.location_lon,
                }

                producer.send(
                    self._settings.kafka_topic_telemetry,
                    key=f"telemetry-{lat}-{lon}",
                    value=message,
                )
                producer.flush()

                logger.info(
                    f"Telemetry sent: wind_speed={latest.wind_speed}, temp={latest.temperature}"
                )

            # Fetch current satellite data
            sat_dataset = await self._nasa_fetcher.fetch_earth_imagery(lat, lon)

            if sat_dataset.atmospheric_data:
                latest_atm = sat_dataset.atmospheric_data[-1]
                message = {
                    "type": "telemetry",
                    "source": "nasa",
                    "timestamp": latest_atm.timestamp.isoformat(),
                    "co2_concentration": latest_atm.co2_concentration,
                    "surface_temperature": latest_atm.surface_temperature,
                    "sea_level_pressure": latest_atm.sea_level_pressure,
                    "location_lat": latest_atm.location_lat,
                    "location_lon": latest_atm.location_lon,
                }

                producer.send(
                    self._settings.kafka_topic_telemetry,
                    key=f"atm-telemetry-{lat}-{lon}",
                    value=message,
                )
                producer.flush()

                logger.info(
                    f"Atmospheric telemetry sent: CO2={latest_atm.co2_concentration}"
                )

            # Wait for next interval
            await asyncio.sleep(interval_seconds)

        logger.info("Real-time telemetry ingestion completed")

    # ==================== Combined Operations ====================

    async def run_full_ingestion(
        self, locations: list[tuple[float, float]], mode: str = "batch"
    ) -> dict[str, Any]:
        """Run full ingestion for multiple locations.

        Args:
            locations: List of (lat, lon) tuples
            mode: "batch" or "realtime"

        Returns:
            Summary of ingestion results
        """
        results: dict[str, Any] = {
            "mode": mode,
            "locations_processed": 0,
            "wind_datasets": 0,
            "satellite_datasets": 0,
            "errors": [],
        }

        for lat, lon in locations:
            try:
                if mode == "batch":
                    await self.ingest_wind_data_batch(lat, lon)
                    await self.ingest_satellite_data_batch(lat, lon)
                    results["wind_datasets"] += 1
                    results["satellite_datasets"] += 1
                elif mode == "realtime":
                    await self.ingest_telemetry_realtime(lat, lon)

                results["locations_processed"] += 1

            except Exception as e:
                logger.error(f"Error processing location ({lat}, {lon}): {e}")
                results["errors"].append({"location": (lat, lon), "error": str(e)})

        return results


async def main() -> None:
    """Main entry point for the ingestion service."""
    logger.info("Starting Data Ingestion Service")

    service = DataIngestionService()

    try:
        # Example: Ingest data for a wind farm location
        locations = [
            (39.7406, -104.9917),  # Denver, CO
            (34.0522, -118.2437),  # Los Angeles, CA
            (41.8781, -87.6298),  # Chicago, IL
        ]

        # Run batch ingestion
        results = await service.run_full_ingestion(locations, mode="batch")
        logger.info(f"Ingestion results: {results}")

    finally:
        await service.close()

    logger.info("Data Ingestion Service stopped")


if __name__ == "__main__":
    asyncio.run(main())
