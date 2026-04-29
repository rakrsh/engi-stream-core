# Data Quality Guide

This guide covers data quality validation using Great Expectations in the Engineering Data Platform.

---

## Overview

The platform implements a **Data Quality** layer that validates all engineering units against realistic bounds before data reaches the database. This ensures data integrity and prevents invalid measurements from corrupting analytics.

---

## Engineering Unit Bounds

### Wind Data

| Parameter | Min | Max | Unit | Description |
|-----------|-----|-----|------|-------------|
| `wind_speed` | 0 | 50 | m/s | Cannot be negative; extreme hurricane is ~50 m/s |
| `wind_direction` | 0 | 360 | degrees | Compass direction |
| `temperature` | -50 | 60 | °C | Extreme cold to hot desert |
| `pressure` | 80,000 | 110,000 | Pa | Atmospheric pressure range |
| `air_density` | 0.9 | 1.5 | kg/m³ | Standard atmosphere density |
| `turbulence_intensity` | 0 | 1 | - | Normalized turbulence |

### Atmospheric Data

| Parameter | Min | Max | Unit | Description |
|-----------|-----|-----|------|-------------|
| `co2_concentration` | 300 | 600 | ppm | Pre-industrial to elevated |
| `ch4_concentration` | 1,500 | 2,500 | ppb | Methane concentration |
| `ozone_concentration` | 100 | 600 | Dobson units | Ozone layer |
| `aerosol_optical_depth` | 0 | 5 | - | Atmospheric clarity |
| `surface_temperature` | 200 | 350 | K | Kelvin (freezing to hot) |
| `sea_level_pressure` | 90,000 | 110,000 | Pa | Sea level pressure |
| `wind_u` | -50 | 50 | m/s | Zonal wind component |
| `wind_v` | -50 | 50 | m/s | Meridional wind component |

### Location Data

| Parameter | Min | Max | Unit |
|-----------|-----|-----|------|
| `location_lat` | -90 | 90 | degrees |
| `location_lon` | -180 | 180 | degrees |

---

## Great Expectations Configuration

### Expectations File

Location: `data-ingestion/quality-control/great_expectations/expectations.py`

```python
ENGINEERING_BOUNDS = {
    "wind_speed": {
        "min": 0.0,
        "max": 50.0,
        "description": "Wind speed must be between 0 and 50 m/s"
    },
    # ... more bounds
}
```

### Running Validation

#### Command Line

```bash
# Run checkpoint
docker exec -it great-expectations gx checkpoint run energy_data_checkpoint

# Run with verbose output
docker exec -it great-expectations gx checkpoint run energy_data_checkpoint --verbose
```

#### Python API

```python
from quality_control.validators.energy_data_validator import DataQualityValidator

validator = DataQualityValidator()

# Validate wind data
wind_result = validator.validate_wind_data(wind_data)

# Validate atmospheric data
atm_result = validator.validate_atmospheric_data(atmospheric_data)

# Run full validation
full_result = validator.run_full_validation(
    wind_data=wind_data,
    atmospheric_data=atmospheric_data
)
```

---

## Validation Results

### Success Response

```json
{
  "success": true,
  "validated_at": "2024-01-15T10:30:00Z",
  "record_count": 1000,
  "expectations_passed": 5,
  "expectations_failed": 0,
  "details": [
    {
      "expectation": "expect_column_values_to_be_between",
      "column": "wind_speed",
      "success": true,
      "unexpected_percent": 0.0
    }
  ]
}
```

### Failure Response

```json
{
  "success": false,
  "validated_at": "2024-01-15T10:30:00Z",
  "record_count": 1000,
  "expectations_passed": 4,
  "expectations_failed": 1,
  "details": [
    {
      "expectation": "expect_column_values_to_be_between",
      "column": "wind_speed",
      "success": false,
      "unexpected_percent": 2.5,
      "unexpected_values": [55.2, 62.1, -5.0]
    }
  ]
}
```

---

## Integration with Airflow

### DAG Integration

The data quality checks are integrated into the Airflow DAG:

```python
def run_data_quality_checks(**context):
    from validators.energy_data_validator import run_validation
    return run_validation()

# In DAG
quality_checks = PythonOperator(
    task_id="run_data_quality_checks",
    python_callable=run_data_quality_checks,
)
```

### Checkpoint Definition

Location: `data-ingestion/quality-control/great_expectations/checkpoints/`

```yaml
name: energy_data_checkpoint
validations:
  - expectation_suite_name: wind_data_quality
  - expectation_suite_name: atmospheric_data_quality
```

---

## Custom Expectations

### Adding New Bounds

Edit `data-ingestion/quality-control/great_expectations/expectations.py`:

```python
ENGINEERING_BOUNDS = {
    # ... existing bounds
    "new_parameter": {
        "min": 0.0,
        "max": 100.0,
        "description": "New parameter bounds"
    },
}
```

### Creating Custom Validator

```python
from great_expectations.dataset import PandasDataset
from great_expectations.core import ExpectationConfiguration

class CustomValidator:
    def validate_custom_data(self, data):
        df = PandasDataset(data)

        expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "custom_column",
                    "min_value": 0.0,
                    "max_value": 100.0
                }
            )
        ]

        for exp in expectations:
            df.expectation_suite.add_expectation(exp)

        return df.validate()
```

---

## Monitoring Quality Metrics

### Prometheus Metrics

```yaml
# Add to docker-compose.yaml
metrics:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
```

### Quality Dashboards

| Metric | Description |
|--------|-------------|
| `data_quality_pass_rate` | Percentage of records passing validation |
| `data_quality_violations` | Count of bound violations |
| `expectation_failure_rate` | Failure rate per expectation |

---

## Troubleshooting

### Common Issues

#### 1. Validation Timeout

```bash
# Increase timeout in settings
VALIDATION_TIMEOUT=300
```

#### 2. Memory Issues

```bash
# Increase container memory
# docker-compose.yaml
ingestion-service:
  mem_limit: 2g
```

#### 3. False Positives

Adjust bounds in `expectations.py` if legitimate values are being flagged:

```python
"wind_speed": {
    "min": 0.0,
    "max": 60.0,  # Increased for high-altitude sites
}
```

---

## Best Practices

1. **Always validate before database insertion**
2. **Use appropriate bounds for your data sources**
3. **Monitor validation failure rates**
4. **Log validation results for auditing**
5. **Implement retry logic for transient failures**
6. **Separate valid and invalid data streams**
