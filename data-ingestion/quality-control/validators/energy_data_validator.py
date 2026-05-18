"""Data quality validator using Great Expectations.

Validates incoming data against defined expectations before
writing to the database.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.logger import get_logger
from config.settings import get_ingestion_settings
from great_expectations import DataContext
from great_expectations.dataset import PandasDataset
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

logger = get_logger(__name__)


class DataQualityValidator:
    """Validates data quality using Great Expectations."""

    def __init__(
        self, context_path: Optional[str] = None, checkpoint_name: Optional[str] = None
    ) -> None:
        """Initialize the validator.

        Args:
            context_path: Path to Great Expectations context
            checkpoint_name: Name of the checkpoint to use
        """
        self._settings = get_ingestion_settings()
        self._context_path = context_path or "/tmp/gx"
        self._checkpoint_name = checkpoint_name or self._settings.great_expectations_checkpoint
        self._context: Optional[DataContext] = None

    def _get_context(self) -> DataContext:
        """Get or create Great Expectations context."""
        if self._context is None:
            try:
                self._context = DataContext.get_or_create(self._context_path)
            except Exception as e:
                logger.warning(f"Could not load existing context: {e}")
                self._context = DataContext.create(self._context_path)
        return self._context

    def validate_wind_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate wind data against expectations.

        Args:
            data: List of wind data records

        Returns:
            Validation result dictionary
        """
        logger.info(f"Validating {len(data)} wind data records")

        # Convert to PandasDataset
        df = PandasDataset(data)

        # Define expectations for wind data
        expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "wind_speed", "min_value": 0.0, "max_value": 50.0},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "wind_direction",
                    "min_value": 0.0,
                    "max_value": 360.0,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "temperature", "min_value": -50.0, "max_value": 60.0},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "pressure", "min_value": 80000, "max_value": 110000},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "wind_speed", "mostly": 0.95},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "timestamp", "mostly": 1.0},
            ),
        ]

        # Run validation
        validation_results = []
        for exp in expectations:
            result = df.expectation_suite.add_expectation(exp)
            validation_results.append(result)

        # Get overall result
        success = all(r.success for r in validation_results) if validation_results else True

        result = {
            "success": success,
            "validated_at": datetime.now().isoformat(),
            "record_count": len(data),
            "expectations_passed": sum(1 for r in validation_results if r.success),
            "expectations_failed": sum(1 for r in validation_results if not r.success),
            "details": [
                {
                    "expectation": r.expectation_config.expectation_type,
                    "column": r.expectation_config.kwargs.get("column"),
                    "success": r.success,
                    "unexpected_percent": r.result.get("unexpected_percent", 0),
                }
                for r in validation_results
            ],
        }

        logger.info(
            f"Validation complete: {result['expectations_passed']} passed, "
            f"{result['expectations_failed']} failed"
        )

        return result  # type: ignore[no-any-return]

    def validate_atmospheric_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate atmospheric data against expectations.

        Args:
            data: List of atmospheric data records

        Returns:
            Validation result dictionary
        """
        logger.info(f"Validating {len(data)} atmospheric data records")

        # Convert to PandasDataset
        df = PandasDataset(data)

        # Define expectations for atmospheric data
        expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "co2_concentration",
                    "min_value": 300.0,
                    "max_value": 600.0,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "ch4_concentration",
                    "min_value": 1500.0,
                    "max_value": 2500.0,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "surface_temperature",
                    "min_value": 200.0,
                    "max_value": 350.0,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "sea_level_pressure",
                    "min_value": 90000,
                    "max_value": 110000,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "co2_concentration", "mostly": 0.90},
            ),
        ]

        # Run validation
        validation_results = []
        for exp in expectations:
            result = df.expectation_suite.add_expectation(exp)
            validation_results.append(result)

        # Get overall result
        success = all(r.success for r in validation_results) if validation_results else True

        result = {
            "success": success,
            "validated_at": datetime.now().isoformat(),
            "record_count": len(data),
            "expectations_passed": sum(1 for r in validation_results if r.success),
            "expectations_failed": sum(1 for r in validation_results if not r.success),
            "details": [
                {
                    "expectation": r.expectation_config.expectation_type,
                    "column": r.expectation_config.kwargs.get("column"),
                    "success": r.success,
                    "unexpected_percent": r.result.get("unexpected_percent", 0),
                }
                for r in validation_results
            ],
        }

        logger.info(
            f"Validation complete: {result['expectations_passed']} passed, "
            f"{result['expectations_failed']} failed"
        )

        return result

    def validate_location_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate location data against expectations.

        Args:
            data: List of records with location data

        Returns:
            Validation result dictionary
        """
        logger.info(f"Validating {len(data)} location records")

        df = PandasDataset(data)

        expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "location_lat",
                    "min_value": -90.0,
                    "max_value": 90.0,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "location_lon",
                    "min_value": -180.0,
                    "max_value": 180.0,
                },
            ),
        ]

        validation_results = []
        for exp in expectations:
            result = df.expectation_suite.add_expectation(exp)
            validation_results.append(result)

        success = all(r.success for r in validation_results)

        return {
            "success": success,
            "validated_at": datetime.now().isoformat(),
            "record_count": len(data),
            "expectations_passed": sum(1 for r in validation_results if r.success),
            "expectations_failed": sum(1 for r in validation_results if not r.success),
        }

    def run_full_validation(
        self,
        wind_data: Optional[List[Dict[str, Any]]] = None,
        atmospheric_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Run full data quality validation.

        Args:
            wind_data: Optional wind data to validate
            atmospheric_data: Optional atmospheric data to validate

        Returns:
            Combined validation results
        """
        results: Dict[str, Any] = {
            "validation_run_at": datetime.now().isoformat(),
            "overall_success": True,
            "validations": [],
        }

        if wind_data:
            wind_result = self.validate_wind_data(wind_data)
            results["validations"].append({"data_type": "wind", "result": wind_result})
            results["overall_success"] = results["overall_success"] and wind_result["success"]

        if atmospheric_data:
            atm_result = self.validate_atmospheric_data(atmospheric_data)
            results["validations"].append({"data_type": "atmospheric", "result": atm_result})
            results["overall_success"] = results["overall_success"] and atm_result["success"]

        logger.info(f"Full validation complete: overall_success={results['overall_success']}")

        return results


def run_validation() -> Dict[str, Any]:
    """Run data quality validation.

    This function is called from Airflow DAGs.
    """
    validator = DataQualityValidator()

    # Example validation with mock data
    mock_wind_data = [
        {
            "timestamp": datetime.now().isoformat(),
            "wind_speed": 12.5,
            "wind_direction": 180.0,
            "temperature": 25.0,
            "pressure": 101325.0,
            "location_lat": 39.7406,
            "location_lon": -104.9917,
        }
    ]

    return validator.validate_wind_data(mock_wind_data)


if __name__ == "__main__":
    # Run validation test
    result = run_validation()
    print(json.dumps(result, indent=2))
