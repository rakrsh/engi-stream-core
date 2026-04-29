"""Great Expectations configuration for energy data.

Defines expectations for wind and atmospheric data quality validation.
"""
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.dataset import PandasDataset, SparkDFDataset

# Data quality thresholds for engineering units
ENGINEERING_BOUNDS = {
    "wind_speed": {
        "min": 0.0,  # m/s - cannot be negative
        "max": 50.0,  # m/s - extreme hurricane force
        "description": "Wind speed must be between 0 and 50 m/s",
    },
    "wind_direction": {
        "min": 0.0,  # degrees
        "max": 360.0,
        "description": "Wind direction must be between 0 and 360 degrees",
    },
    "temperature": {
        "min": -50.0,  # Celsius
        "max": 60.0,  # Celsius
        "description": "Temperature must be between -50 and 60 Celsius",
    },
    "pressure": {
        "min": 80000,  # Pa
        "max": 110000,  # Pa
        "description": "Atmospheric pressure must be between 80k and 110k Pa",
    },
    "air_density": {
        "min": 0.9,  # kg/m³
        "max": 1.5,  # kg/m³
        "description": "Air density must be between 0.9 and 1.5 kg/m³",
    },
    "co2_concentration": {
        "min": 300.0,  # ppm
        "max": 600.0,  # ppm
        "description": "CO2 concentration must be between 300 and 600 ppm",
    },
    "ch4_concentration": {
        "min": 1500.0,  # ppb
        "max": 2500.0,  # ppb
        "description": "CH4 concentration must be between 1500 and 2500 ppb",
    },
    "ozone_concentration": {
        "min": 100.0,  # Dobson units
        "max": 600.0,  # Dobson units
        "description": "Ozone concentration must be between 100 and 600 Dobson units",
    },
    "aerosol_optical_depth": {
        "min": 0.0,
        "max": 5.0,
        "description": "Aerosol optical depth must be between 0 and 5",
    },
    "surface_temperature": {
        "min": 200.0,  # Kelvin
        "max": 350.0,  # Kelvin
        "description": "Surface temperature must be between 200 and 350 K",
    },
    "sea_level_pressure": {
        "min": 90000,  # Pa
        "max": 110000,  # Pa
        "description": "Sea level pressure must be between 90k and 110k Pa",
    },
    "wind_u": {
        "min": -50.0,  # m/s
        "max": 50.0,  # m/s
        "description": "Zonal wind component must be between -50 and 50 m/s",
    },
    "wind_v": {
        "min": -50.0,  # m/s
        "max": 50.0,  # m/s
        "description": "Meridional wind component must be between -50 and 50 m/s",
    },
}


def get_wind_data_expectations() -> List[ExpectationConfiguration]:
    """Get expectations for wind data validation."""
    return [
        # Column-level expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "wind_speed", "type_": "FloatType"},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "wind_direction", "type_": "FloatType"},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "temperature", "type_": "FloatType"},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "pressure", "type_": "FloatType"},
        ),
        # Value bounds expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "wind_speed",
                "min_value": ENGINEERING_BOUNDS["wind_speed"]["min"],
                "max_value": ENGINEERING_BOUNDS["wind_speed"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "wind_direction",
                "min_value": ENGINEERING_BOUNDS["wind_direction"]["min"],
                "max_value": ENGINEERING_BOUNDS["wind_direction"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "temperature",
                "min_value": ENGINEERING_BOUNDS["temperature"]["min"],
                "max_value": ENGINEERING_BOUNDS["temperature"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "pressure",
                "min_value": ENGINEERING_BOUNDS["pressure"]["min"],
                "max_value": ENGINEERING_BOUNDS["pressure"]["max"],
            },
        ),
        # Not null expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "wind_speed", "mostly": 0.95},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "timestamp", "mostly": 1.0},
        ),
        # Unique expectations
        ExpectationConfiguration(
            expectation_type="expect_column_unique_value_count_to_be",
            kwargs={"column": "dataset_id", "value": 1},
        ),
    ]


def get_atmospheric_data_expectations() -> List[ExpectationConfiguration]:
    """Get expectations for atmospheric data validation."""
    return [
        # Column-level expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "co2_concentration", "type_": "FloatType"},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "surface_temperature", "type_": "FloatType"},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "sea_level_pressure", "type_": "FloatType"},
        ),
        # Value bounds expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "co2_concentration",
                "min_value": ENGINEERING_BOUNDS["co2_concentration"]["min"],
                "max_value": ENGINEERING_BOUNDS["co2_concentration"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "ch4_concentration",
                "min_value": ENGINEERING_BOUNDS["ch4_concentration"]["min"],
                "max_value": ENGINEERING_BOUNDS["ch4_concentration"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "surface_temperature",
                "min_value": ENGINEERING_BOUNDS["surface_temperature"]["min"],
                "max_value": ENGINEERING_BOUNDS["surface_temperature"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "sea_level_pressure",
                "min_value": ENGINEERING_BOUNDS["sea_level_pressure"]["min"],
                "max_value": ENGINEERING_BOUNDS["sea_level_pressure"]["max"],
            },
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "aerosol_optical_depth",
                "min_value": ENGINEERING_BOUNDS["aerosol_optical_depth"]["min"],
                "max_value": ENGINEERING_BOUNDS["aerosol_optical_depth"]["max"],
            },
        ),
        # Not null expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "co2_concentration", "mostly": 0.90},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "timestamp", "mostly": 1.0},
        ),
    ]


def get_location_expectations() -> List[ExpectationConfiguration]:
    """Get expectations for location data validation."""
    return [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "location_lat", "min_value": -90.0, "max_value": 90.0},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "location_lon", "min_value": -180.0, "max_value": 180.0},
        ),
    ]


def create_expectation_suite(
    suite_name: str, data_type: str = "wind"
) -> Dict[str, Any]:
    """Create a Great Expectations expectation suite.

    Args:
        suite_name: Name of the expectation suite
        data_type: Type of data ("wind" or "atmospheric")

    Returns:
        Dictionary containing the expectation suite configuration
    """
    if data_type == "wind":
        expectations = get_wind_data_expectations()
    elif data_type == "atmospheric":
        expectations = get_atmospheric_data_expectations()
    else:
        expectations = []

    # Add location expectations to all suites
    expectations.extend(get_location_expectations())

    return {
        "expectation_suite_name": suite_name,
        "expectations": [e.to_json_dict() for e in expectations],
        "ge_cloud_id": None,
        "meta": {
            "great_expectations_version": "0.17.0",
            "data_type": data_type,
            "created_at": datetime.now().isoformat(),
            "created_by": "data-engineering-team",
        },
    }


# Default suite names
WIND_DATA_SUITE = "wind_data_quality"
ATMOSPHERIC_DATA_SUITE = "atmospheric_data_quality"
ENERGY_DATA_SUITE = "energy_data_checkpoint"


if __name__ == "__main__":
    # Print sample expectation suite
    import json

    suite = create_expectation_suite(WIND_DATA_SUITE, "wind")
    print(json.dumps(suite, indent=2))
