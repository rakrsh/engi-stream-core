"""Ingestion Service Configuration.

Loads configuration from environment variables following 12-Factor App principles.
"""
from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    """Configuration for the data ingestion service."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # NREL API Configuration
    nrel_api_key: str = Field(default="", description="NREL API Key for wind data")
    nrel_base_url: str = Field(
        default="https://developer.nrel.gov/api/wind",
        description="NREL API base URL"
    )
    nrel_endpoint: str = Field(
        default="/toolkit/wind-toolkit",
        description="NREL wind toolkit endpoint"
    )

    # NASA API Configuration
    nasa_api_key: str = Field(default="DEMO_KEY", description="NASA API Key")
    nasa_base_url: str = Field(
        default="https://api.nasa.gov",
        description="NASA API base URL"
    )
    nasa_satellite_endpoint: str = Field(
        default="/planetary/earth/imagery",
        description="NASA satellite imagery endpoint"
    )

    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka broker addresses"
    )
    kafka_topic_telemetry: str = Field(
        default="telemetry.raw",
        description="Kafka topic for real-time telemetry"
    )
    kafka_topic_batch: str = Field(
        default="batch.raw",
        description="Kafka topic for batch data"
    )
    kafka_consumer_group: str = Field(
        default="ingestion-service",
        description="Kafka consumer group ID"
    )

    # Database Configuration
    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/energy_data",
        description="PostgreSQL database URL"
    )

    # Quality Control
    great_expectations_checkpoint: str = Field(
        default="energy_data_checkpoint",
        description="Great Expectations checkpoint name"
    )

    # Service Configuration
    batch_size: int = Field(default=1000, description="Batch size for processing")
    request_timeout: int = Field(default=30, description="HTTP request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: int = Field(default=5, description="Retry delay in seconds")


@lru_cache
def get_ingestion_settings() -> IngestionSettings:
    """Get cached ingestion settings instance."""
    return IngestionSettings()