"""Spark Job for processing wind data.

Processes raw wind data from Kafka, applies transformations,
and writes to the data warehouse.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_json, from_json, window, avg, min as spark_min,
    max as spark_max, stddev, count, when, lit, udf, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    IntegerType, FloatType
)


# Define schema for wind data
WIND_DATA_SCHEMA = StructType([
    StructField("dataset_id", StringType(), True),
    StructField("location_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("wind_direction_cardinal", StringType(), True),
    StructField("turbulence_intensity", DoubleType(), True),
    StructField("air_density", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("hub_height", DoubleType(), True),
    StructField("location_lat", DoubleType(), True),
    StructField("location_lon", DoubleType(), True),
    StructField("fetched_at", StringType(), True),
])


@dataclass
class WindDataProcessor:
    """Processor for wind data using Spark."""

    spark: SparkSession
    kafka_bootstrap_servers: str = "localhost:9092"
    checkpoint_location: str = "/tmp/spark/checkpoints"

    def __post_init__(self) -> None:
        self._setup_spark_config()

    def _setup_spark_config(self) -> None:
        """Configure Spark session."""
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", 
                           self.checkpoint_location)

    def read_from_kafka(self, topic: str = "batch.raw") -> DataFrame:
        """Read wind data from Kafka."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def parse_wind_data(self, df: DataFrame) -> DataFrame:
        """Parse and transform wind data."""
        # Parse JSON value
        parsed = df.select(
            from_json(col("value").cast("string"), WIND_DATA_SCHEMA).alias("data")
        ).select("data.*")

        # Add processing timestamp
        parsed = parsed.withColumn("processed_at", to_timestamp(lit(datetime.now().isoformat())))

        # Calculate derived fields
        parsed = parsed.withColumn(
            "wind_power_density",
            col("air_density") * col("wind_speed") ** 3 / 2
        )

        # Add quality flag
        parsed = parsed.withColumn(
            "quality_flag",
            when(
                (col("wind_speed") < 0) | (col("wind_speed") > 50), "invalid"
            ).when(
                (col("temperature") < -50) | (col("temperature") > 60), "invalid"
            ).when(
                (col("pressure") < 80000) | (col("pressure") > 110000), "invalid"
            ).otherwise("valid")
        )

        return parsed

    def aggregate_hourly(self, df: DataFrame) -> DataFrame:
        """Calculate hourly aggregations."""
        return (
            df.withColumn("hour", window(col("timestamp"), "1 hour"))
            .groupBy("hour", "location_lat", "location_lon")
            .agg(
                avg("wind_speed").alias("avg_wind_speed"),
                spark_min("wind_speed").alias("min_wind_speed"),
                spark_max("wind_speed").alias("max_wind_speed"),
                stddev("wind_speed").alias("stddev_wind_speed"),
                avg("temperature").alias("avg_temperature"),
                avg("pressure").alias("avg_pressure"),
                avg("air_density").alias("avg_air_density"),
                count("*").alias("record_count")
            )
        )

    def write_to_console(self, df: DataFrame) -> None:
        """Write processed data to console."""
        query = (
            df.writeStream
            .format("console")
            .outputMode("complete")
            .option("truncate", False)
            .start()
        )
        query.awaitTermination()

    def write_to_parquet(self, df: DataFrame, output_path: str) -> None:
        """Write processed data to Parquet files."""
        query = (
            df.writeStream
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{self.checkpoint_location}/parquet")
            .outputMode("append")
            .start()
        )
        query.awaitTermination()

    def write_to_postgresql(self, df: DataFrame, table: str) -> None:
        """Write processed data to PostgreSQL."""
        query = (
            df.writeStream
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/energy_data")
            .option("dbtable", table)
            .option("user", "postgres")
            .option("password", "postgres")
            .option("driver", "org.postgresql.Driver")
            .outputMode("append")
            .start()
        )
        query.awaitTermination()


def run_wind_processing_job(
    kafka_servers: str = "localhost:9092",
    output_path: str = "/tmp/wind_data",
    mode: str = "batch"
) -> None:
    """Run the wind data processing job.

    Args:
        kafka_servers: Kafka bootstrap servers
        output_path: Output path for processed data
        mode: "batch" or "streaming"
    """
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("WindDataProcessing")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark/checkpoints")
        .getOrCreate()
    )

    processor = WindDataProcessor(spark, kafka_servers)

    # Read from Kafka
    raw_df = processor.read_from_kafka("batch.raw")

    # Parse wind data
    wind_df = processor.parse_wind_data(raw_df)

    # Filter for wind data only
    wind_data = wind_df.filter(col("wind_speed").isNotNull())

    if mode == "streaming":
        # Streaming mode: write to console
        processor.write_to_console(wind_data)
    else:
        # Batch mode: aggregate and write to parquet
        hourly_agg = processor.aggregate_hourly(wind_data)
        processor.write_to_parquet(hourly_agg, output_path)


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    run_wind_processing_job(mode=mode)