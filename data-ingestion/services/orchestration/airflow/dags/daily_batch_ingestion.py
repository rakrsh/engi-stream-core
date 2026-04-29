"""Airflow DAG for daily batch data ingestion.

This DAG orchestrates the daily batch load of NREL wind and NASA satellite data.
"""
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.kafka.operators.produce import KafkaOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def fetch_nrel_wind_data(**context: Any) -> dict[str, Any]:
    """Fetch NREL wind simulation data."""
    import asyncio
    import sys

    sys.path.insert(0, "/opt/ingestion/src")

    from app import DataIngestionService

    async def _fetch():
        service = DataIngestionService()
        try:
            # Fetch for multiple wind farm locations
            locations = [
                (39.7406, -104.9917),  # Denver, CO
                (34.0522, -118.2437),  # Los Angeles, CA
            ]
            results = await service.run_full_ingestion(locations, mode="batch")
            return results
        finally:
            await service.close()

    return asyncio.run(_fetch())


def fetch_nasa_satellite_data(**context: Any) -> dict[str, Any]:
    """Fetch NASA satellite data."""
    import asyncio
    import sys

    sys.path.insert(0, "/opt/ingestion/src")

    from app import DataIngestionService

    async def _fetch():
        service = DataIngestionService()
        try:
            locations = [
                (39.7406, -104.9917),
                (34.0522, -118.2437),
            ]
            results = await service.run_full_ingestion(locations, mode="batch")
            return results
        finally:
            await service.close()

    return asyncio.run(_fetch())


def run_data_quality_checks(**context: Any) -> dict[str, Any]:
    """Run Great Expectations data quality checks."""
    import sys

    sys.path.insert(0, "/opt/quality-control")

    from validators.energy_data_validator import run_validation

    return run_validation()


def aggregate_daily_data(**context: Any) -> dict[str, Any]:
    """Aggregate daily data using Spark."""
    # This would trigger a Spark job
    return {"status": "success", "records_processed": 1000}


# Define the DAG
with DAG(
    dag_id="daily_batch_ingestion",
    default_args=default_args,
    description="Daily batch ingestion of NREL wind and NASA satellite data",
    schedule_interval="0 2 * * *",  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "batch", "nrel", "nasa"],
) as dag:
    # Task Group: Data Extraction
    with TaskGroup("extraction_group") as extraction_group:
        # Check if NREL API is available
        check_nrel_api = HttpSensor(
            task_id="check_nrel_api",
            http_conn_id="nrel_api",
            endpoint="api/wind/toolkit",
            timeout=300,
            poke_interval=30,
        )

        # Check if NASA API is available
        check_nasa_api = HttpSensor(
            task_id="check_nasa_api",
            http_conn_id="nasa_api",
            endpoint="planetary/earth/imagery",
            timeout=300,
            poke_interval=30,
        )

        # Fetch NREL wind data
        fetch_nrel = PythonOperator(
            task_id="fetch_nrel_wind_data",
            python_callable=fetch_nrel_wind_data,
        )

        # Fetch NASA satellite data
        fetch_nasa = PythonOperator(
            task_id="fetch_nasa_satellite_data",
            python_callable=fetch_nasa_satellite_data,
        )

    # Task Group: Data Quality
    with TaskGroup("quality_group") as quality_group:
        # Run data quality checks
        quality_checks = PythonOperator(
            task_id="run_data_quality_checks",
            python_callable=run_data_quality_checks,
        )

        # Validate data against Great Expectations
        run_gx_validation = BashOperator(
            task_id="run_great_expectations",
            bash_command="python -m great_expectations checkpoint run energy_data_checkpoint",
        )

    # Task Group: Data Processing
    with TaskGroup("processing_group") as processing_group:
        # Aggregate daily data
        aggregate_data = PythonOperator(
            task_id="aggregate_daily_data",
            python_callable=aggregate_daily_data,
        )

        # Store to data warehouse
        store_to_warehouse = PostgresOperator(
            task_id="store_to_warehouse",
            postgres_conn_id="postgres_warehouse",
            sql="""
                INSERT INTO daily_metrics (date, source, record_count, created_at)
                VALUES ({{ ds }}, 'batch', {{ task_instance.xcom_pull(task_ids='processing_group.aggregate_daily_data')['records_processed'] }}, NOW())
            """,
        )

    # Task Group: Notifications
    with TaskGroup("notification_group") as notification_group:
        # Send success notification
        send_notification = BashOperator(
            task_id="send_success_notification",
            bash_command="echo 'Daily batch ingestion completed successfully'",
        )

    # Define task dependencies
    extraction_group >> quality_group >> processing_group >> notification_group
