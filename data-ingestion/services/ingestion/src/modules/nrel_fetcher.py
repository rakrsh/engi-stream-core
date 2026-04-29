"""NREL Wind Simulation Data Fetcher.

Fetches wind simulation data from the NREL (National Renewable Energy Laboratory)
API. Handles wind speed, direction, and other meteorological data.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import aiohttp
from config.logger import get_logger
from config.settings import get_ingestion_settings

logger = get_logger(__name__)


@dataclass
class WindDataPoint:
    """Represents a single wind data measurement."""

    timestamp: datetime
    wind_speed: float  # m/s
    wind_direction: float  # degrees
    wind_direction_cardinal: str  # N, NE, E, etc.
    turbulence_intensity: Optional[float] = None
    air_density: Optional[float] = None  # kg/m³
    temperature: Optional[float] = None  # Celsius
    pressure: Optional[float] = None  # Pa
    hub_height: Optional[float] = None  # meters
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None


@dataclass
class WindDataset:
    """Represents a complete wind dataset from NREL."""

    dataset_id: str
    location_name: str
    data_points: list[WindDataPoint]
    fetched_at: datetime
    metadata: dict[str, Any]


class NRELWindFetcher:
    """Fetches wind simulation data from NREL API."""

    def __init__(self) -> None:
        self._settings = get_ingestion_settings()
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._settings.request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    def _degrees_to_cardinal(self, degrees: float) -> str:
        """Convert degrees to cardinal direction."""
        directions = [
            "N",
            "NNE",
            "NE",
            "ENE",
            "E",
            "ESE",
            "SE",
            "SSE",
            "S",
            "SSW",
            "SW",
            "WSW",
            "W",
            "WNW",
            "NW",
            "NNW",
        ]
        index = round(degrees / 22.5) % 16
        return directions[index]

    async def fetch_wind_data(
        self,
        lat: float,
        lon: float,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        hub_height: float = 80.0,
    ) -> WindDataset:
        """Fetch wind data for a given location.

        Args:
            lat: Latitude of the location
            lon: Longitude of the location
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            hub_height: Turbine hub height in meters

        Returns:
            WindDataset containing the fetched wind data
        """
        settings = self._settings
        session = await self._get_session()

        # Build API request parameters
        params = {
            "api_key": settings.nrel_api_key or "DEMO_KEY",
            "lat": lat,
            "lon": lon,
            "hub_height": hub_height,
            "attr": "wind_speed,wind_direction,air_density,temperature,pressure",
        }

        if start_date:
            params["start_date"] = start_date.isoformat()
        if end_date:
            params["end_date"] = end_date.isoformat()

        logger.info(f"Fetching NREL wind data for ({lat}, {lon})")

        try:
            async with session.get(
                f"{settings.nrel_base_url}{settings.nrel_endpoint}", params=params
            ) as response:
                response.raise_for_status()
                data = await response.json()

                return self._parse_wind_response(data, lat, lon, hub_height)

        except aiohttp.ClientError as e:
            logger.error(f"Failed to fetch NREL wind data: {e}")
            # Return mock data for development when API is unavailable
            return self._generate_mock_data(lat, lon, hub_height)

    def _parse_wind_response(
        self, data: dict[str, Any], lat: float, lon: float, hub_height: float
    ) -> WindDataset:
        """Parse the API response into a WindDataset."""
        data_points = []

        # Extract outputs from response
        outputs = data.get("outputs", {})
        times = outputs.get("time", [])
        wind_speeds = outputs.get("wind_speed", [])
        wind_directions = outputs.get("wind_direction", [])

        for i, timestamp in enumerate(times):
            data_point = WindDataPoint(
                timestamp=datetime.fromisoformat(timestamp)
                if isinstance(timestamp, str)
                else datetime.now(),
                wind_speed=wind_speeds[i] if i < len(wind_speeds) else 0.0,
                wind_direction=wind_directions[i] if i < len(wind_directions) else 0.0,
                wind_direction_cardinal=self._degrees_to_cardinal(
                    wind_directions[i] if i < len(wind_directions) else 0.0
                ),
                air_density=outputs.get("air_density", [None] * len(times))[i]
                if i < len(outputs.get("air_density", []))
                else None,
                temperature=outputs.get("temperature", [None] * len(times))[i]
                if i < len(outputs.get("temperature", []))
                else None,
                pressure=outputs.get("pressure", [None] * len(times))[i]
                if i < len(outputs.get("pressure", []))
                else None,
                hub_height=hub_height,
                location_lat=lat,
                location_lon=lon,
            )
            data_points.append(data_point)

        return WindDataset(
            dataset_id=data.get("meta", {}).get("dataset_id", "unknown"),
            location_name=data.get("meta", {}).get("location", "Unknown"),
            data_points=data_points,
            fetched_at=datetime.now(),
            metadata=data.get("meta", {}),
        )

    def _generate_mock_data(
        self, lat: float, lon: float, hub_height: float
    ) -> WindDataset:
        """Generate mock wind data for development/testing."""
        import random

        data_points = []
        base_time = datetime.now()

        for i in range(24):  # 24 hours of data
            hour_offset = i
            wind_speed = round(random.uniform(3.0, 15.0), 2)
            wind_direction = round(random.uniform(0, 360), 1)

            data_point = WindDataPoint(
                timestamp=base_time.replace(hour=hour_offset % 24),
                wind_speed=wind_speed,
                wind_direction=wind_direction,
                wind_direction_cardinal=self._degrees_to_cardinal(wind_direction),
                turbulence_intensity=round(random.uniform(0.1, 0.5), 3),
                air_density=round(random.uniform(1.1, 1.3), 3),
                temperature=round(random.uniform(-10, 35), 1),
                pressure=round(random.uniform(95000, 105000), 0),
                hub_height=hub_height,
                location_lat=lat,
                location_lon=lon,
            )
            data_points.append(data_point)

        logger.info(f"Generated {len(data_points)} mock wind data points")

        return WindDataset(
            dataset_id="mock-wind-data",
            location_name=f"Location ({lat}, {lon})",
            data_points=data_points,
            fetched_at=datetime.now(),
            metadata={"source": "mock", "hub_height": hub_height},
        )

    async def fetch_multiple_locations(
        self, locations: list[tuple[float, float]], hub_height: float = 80.0
    ) -> list[WindDataset]:
        """Fetch wind data for multiple locations.

        Args:
            locations: List of (lat, lon) tuples
            hub_height: Turbine hub height in meters

        Returns:
            List of WindDatasets
        """
        datasets = []

        for lat, lon in locations:
            dataset = await self.fetch_wind_data(lat, lon, hub_height=hub_height)
            datasets.append(dataset)

        return datasets


# Module-level async context manager for convenience
class NRELWindFetcherContext:
    """Async context manager for NRELWindFetcher."""

    def __init__(self) -> None:
        self._fetcher: Optional[NRELWindFetcher] = None

    async def __aenter__(self) -> NRELWindFetcher:
        self._fetcher = NRELWindFetcher()
        return self._fetcher

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._fetcher:
            await self._fetcher.close()
