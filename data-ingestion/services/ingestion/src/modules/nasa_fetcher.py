"""NASA Satellite Data Fetcher.

Fetches satellite imagery and related data from NASA APIs.
Handles Earth imagery, climate data, and atmospheric measurements.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import aiohttp
from config.logger import get_logger
from config.settings import get_ingestion_settings

logger = get_logger(__name__)


@dataclass
class SatelliteImage:
    """Represents a satellite image metadata."""

    image_id: str
    capture_date: datetime
    cloud_cover: float  # percentage
    location_lat: float
    location_lon: float
    image_url: str
    thumbnail_url: str
    source: str  # e.g., "Landsat8", "Suomi NPP"
    band_info: dict[str, str]


@dataclass
class AtmosphericData:
    """Represents atmospheric measurement data."""

    timestamp: datetime
    co2_concentration: Optional[float] = None  # ppm
    ch4_concentration: Optional[float] = None  # ppb
    ozone_concentration: Optional[float] = None  # Dobson units
    aerosol_optical_depth: Optional[float] = None
    surface_temperature: Optional[float] = None  # Kelvin
    sea_level_pressure: Optional[float] = None  # Pa
    wind_u: Optional[float] = None  # m/s (zonal)
    wind_v: Optional[float] = None  # m/s (meridional)
    location_lat: Optional[float] = None
    location_lon: Optional[float] = None


@dataclass
class SatelliteDataset:
    """Represents a complete satellite dataset from NASA."""

    dataset_id: str
    satellite_name: str
    images: list[SatelliteImage]
    atmospheric_data: list[AtmosphericData]
    fetched_at: datetime
    metadata: dict[str, Any]


class NASASatelliteFetcher:
    """Fetches satellite data from NASA APIs."""

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

    async def fetch_earth_imagery(
        self,
        lat: float,
        lon: float,
        date: Optional[datetime] = None,
        dim: float = 0.025,  # resolution in degrees
    ) -> SatelliteDataset:
        """Fetch satellite earth imagery for a given location.

        Args:
            lat: Latitude of the location
            lon: Longitude of the location
            date: Date for imagery (defaults to most recent)
            dim: Image dimension in degrees

        Returns:
            SatelliteDataset containing satellite imagery
        """
        settings = self._settings
        session = await self._get_session()

        date_str = (
            date.strftime("%Y-%m-%d") if date else datetime.now().strftime("%Y-%m-%d")
        )

        params = {
            "api_key": settings.nasa_api_key,
            "lat": lat,
            "lon": lon,
            "date": date_str,
            "dim": dim,
        }

        logger.info(f"Fetching NASA satellite imagery for ({lat}, {lon}) on {date_str}")

        try:
            async with session.get(
                f"{settings.nasa_base_url}{settings.nasa_satellite_endpoint}",
                params=params,
            ) as response:
                response.raise_for_status()
                data = await response.json()

                return self._parse_imagery_response(data, lat, lon)

        except aiohttp.ClientError as e:
            logger.error(f"Failed to fetch NASA satellite data: {e}")
            # Return mock data for development
            return self._generate_mock_imagery(lat, lon)

    def _parse_imagery_response(
        self, data: dict[str, Any], lat: float, lon: float
    ) -> SatelliteDataset:
        """Parse the API response into a SatelliteDataset."""
        images = []
        atmospheric_data = []

        # Parse image data
        if "url" in data:
            image = SatelliteImage(
                image_id=data.get("id", "unknown"),
                capture_date=datetime.fromisoformat(
                    data.get("date", datetime.now().isoformat())
                ),
                cloud_cover=data.get("cloud_cover", 0.0),
                location_lat=lat,
                location_lon=lon,
                image_url=data.get("url", ""),
                thumbnail_url=data.get("thumbnail", ""),
                source=data.get("source", "NASA"),
                band_info=data.get("bands", {}),
            )
            images.append(image)

        # Parse atmospheric data if available
        if "atmospheric_data" in data:
            for atm_data in data["atmospheric_data"]:
                atmospheric_data.append(
                    AtmosphericData(
                        timestamp=datetime.fromisoformat(
                            atm_data.get("timestamp", datetime.now().isoformat())
                        ),
                        co2_concentration=atm_data.get("co2"),
                        ch4_concentration=atm_data.get("ch4"),
                        ozone_concentration=atm_data.get("ozone"),
                        aerosol_optical_depth=atm_data.get("aod"),
                        surface_temperature=atm_data.get("surface_temp"),
                        sea_level_pressure=atm_data.get("sea_level_pressure"),
                        wind_u=atm_data.get("wind_u"),
                        wind_v=atm_data.get("wind_v"),
                        location_lat=lat,
                        location_lon=lon,
                    )
                )

        return SatelliteDataset(
            dataset_id=data.get("id", "unknown"),
            satellite_name=data.get("source", "NASA Satellite"),
            images=images,
            atmospheric_data=atmospheric_data,
            fetched_at=datetime.now(),
            metadata=data,
        )

    def _generate_mock_imagery(self, lat: float, lon: float) -> SatelliteDataset:
        """Generate mock satellite data for development/testing."""
        import random

        base_time = datetime.now()

        # Generate mock images
        images = [
            SatelliteImage(
                image_id=f"LANDSAT8-{base_time.strftime('%Y%m%d')}-{i:03d}",
                capture_date=base_time,
                cloud_cover=round(random.uniform(0, 30), 1),
                location_lat=lat + random.uniform(-0.01, 0.01),
                location_lon=lon + random.uniform(-0.01, 0.01),
                image_url=f"https://api.nasa.gov/image/{i}",
                thumbnail_url=f"https://api.nasa.gov/thumb/{i}",
                source="Landsat8",
                band_info={"visible": "true", "infrared": "true", "thermal": "true"},
            )
            for i in range(5)
        ]

        # Generate mock atmospheric data
        atmospheric_data = [
            AtmosphericData(
                timestamp=base_time.replace(hour=i),
                co2_concentration=round(random.uniform(400, 420), 2),
                ch4_concentration=round(random.uniform(1800, 1900), 1),
                ozone_concentration=round(random.uniform(250, 350), 1),
                aerosol_optical_depth=round(random.uniform(0.05, 0.3), 3),
                surface_temperature=round(random.uniform(280, 300), 1),
                sea_level_pressure=round(random.uniform(101000, 102000), 0),
                wind_u=round(random.uniform(-10, 10), 2),
                wind_v=round(random.uniform(-10, 10), 2),
                location_lat=lat,
                location_lon=lon,
            )
            for i in range(24)
        ]

        logger.info(
            f"Generated {len(images)} mock satellite images and {len(atmospheric_data)} atmospheric data points"
        )

        return SatelliteDataset(
            dataset_id=f"mock-sat-{base_time.strftime('%Y%m%d')}",
            satellite_name="Landsat8 (Mock)",
            images=images,
            atmospheric_data=atmospheric_data,
            fetched_at=datetime.now(),
            metadata={"source": "mock", "lat": lat, "lon": lon},
        )

    async def fetch_climate_data(
        self,
        start_date: datetime,
        end_date: datetime,
        location: Optional[tuple[float, float]] = None,
    ) -> dict[str, Any]:
        """Fetch climate data for a date range.

        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            location: Optional (lat, lon) tuple

        Returns:
            Dictionary containing climate data
        """
        settings = self._settings
        session = await self._get_session()

        params = {
            "api_key": settings.nasa_api_key,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        if location:
            params["lat"] = location[0]
            params["lon"] = location[1]

        logger.info(f"Fetching NASA climate data from {start_date} to {end_date}")

        try:
            async with session.get(
                f"{settings.nasa_base_url}/planetary/climate", params=params
            ) as response:
                response.raise_for_status()
                return await response.json()

        except aiohttp.ClientError as e:
            logger.error(f"Failed to fetch NASA climate data: {e}")
            return {"error": str(e), "data": []}


# Module-level async context manager for convenience
class NASASatelliteFetcherContext:
    """Async context manager for NASASatelliteFetcher."""

    def __init__(self) -> None:
        self._fetcher: Optional[NASASatelliteFetcher] = None

    async def __aenter__(self) -> NASASatelliteFetcher:
        self._fetcher = NASASatelliteFetcher()
        return self._fetcher

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._fetcher:
            await self._fetcher.close()
