"""Logging configuration for the ingestion service."""
import logging
import os
import sys
from typing import Any

from config.settings import get_ingestion_settings


class IngestionLogger:
    """Custom logger for the ingestion service."""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        self._configure()

    def _configure(self) -> None:
        """Configure logger based on environment."""
        settings = get_ingestion_settings()

        # Set level based on environment
        level = logging.INFO
        if os.getenv("ENV", "production") == "development":
            level = logging.DEBUG

        self._logger.setLevel(level)

        # Avoid duplicate handlers
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)

            # Use JSON format in production, human-readable in dev
            if os.getenv("ENV", "production") == "production":
                formatter = logging.Formatter(
                    '{"time": "%(asctime)s", "level": "%(levelname)s", '
                    '"name": "%(name)s", "message": "%(message)s"}'
                )
            else:
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )

            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._logger.debug(message, extra=kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self._logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self._logger.error(message, extra=kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self._logger.critical(message, extra=kwargs)


def get_logger(name: str) -> IngestionLogger:
    """Get a logger instance for the given name."""
    return IngestionLogger(name)
