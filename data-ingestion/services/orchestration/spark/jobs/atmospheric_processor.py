"""Spark Job for processing satellite/atmospheric data.

Processes raw atmospheric data from Kafka, applies transformations,
and writes to the data warehouse.
"""
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_json, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import to_timestamp, when, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Define schema for atmospheric data
ATMOSPHERIC_DATA_SCHEMA = StructType(
    [
        StructField("dataset_id", StringType(), True),
        StructField("satellite_name", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("co2_concentration", DoubleType(), True),
        StructField("ch4_concentration", DoubleType(), True),
        StructField("ozone_concentration", DoubleType(), True),
        StructField("aerosol_optical_depth", DoubleType(), True),
        StructField("surface_temperature", DoubleType(), True),
        StructField("sea_level_pressure", DoubleType(), True),
        StructField("wind_u", DoubleType(), True),
        StructField("wind_v", DoubleType(), True),
        StructField("location_lat", DoubleType(), True),
        StructField("location_lon", DoubleType(), True),
        StructField("fetched_at", StringType(), True),
    ]
)


@dataclass
class AtmosphericDataProcessor:
    """Processor for atmospheric data using Spark."""

    spark: SparkSession
    kafka_bootstrap_servers: str = "localhost:9092"
    checkpoint_location: str = os.environ.get(
        "SPARK_CHECKPOINT_DIR",
        os.path.join(tempfile.gettempdir(), "spark", "checkpoints"),
    )

    def __post_init__(self) -> None:
        """Initialize processor after dataclass initialization."""
        self._setup_spark_config()

    def _setup_spark_config(self) -> None:
        """Configure Spark session."""
        self.spark.conf.set(
            "spark.sql.streaming.checkpointLocation", self.checkpoint_location
        )

    def read_from_kafka(self, topic: str = "batch.raw") -> DataFrame:
        """Read atmospheric data from Kafka."""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def parse_atmospheric_data(self, df: DataFrame) -> DataFrame:
        """Parse and transform atmospheric data."""
        # Parse JSON value
        parsed = df.select(
            from_json(col("value").cast("string"), ATMOSPHERIC_DATA_SCHEMA).alias(
                "data"
            )
        ).select("data.*")

        # Add processing timestamp
        parsed = parsed.withColumn(
            "processed_at", to_timestamp(lit(datetime.now().isoformat()))
        )

        # Calculate derived fields
        # Wind magnitude from u and v components
        parsed = parsed.withColumn(
            "wind_magnitude", (col("wind_u") ** 2 + col("wind_v") ** 2) ** 0.5
        )

        # Add quality flag
        parsed = parsed.withColumn(
            "quality_flag",
            when(
                (col("co2_concentration") < 300) | (col("co2_concentration") > 600),
                "invalid",
            )
            .when(
                (col("surface_temperature") < 200) | (col("surface_temperature") > 350),
                "invalid",
            )
            .when(
                (col("sea_level_pressure") < 90000)
                | (col("sea_level_pressure") > 110000),
                "invalid",
            )
            .otherwise("valid"),
        )

        return parsed

    def aggregate_daily(self, df: DataFrame) -> DataFrame:
        """Calculate daily aggregations."""
        return (
            df.withColumn("day", window(col("timestamp"), "1 day"))
            .groupBy("day", "location_lat", "location_lon")
            .agg(
                avg("co2_concentration").alias("avg_co2"),
                spark_min("co2_concentration").alias("min_co2"),
                spark_max("co2_concentration").alias("max_co2"),
                avg("ch4_concentration").alias("avg_ch4"),
                avg("ozone_concentration").alias("avg_ozone"),
                avg("aerosol_optical_depth").alias("avg_aod"),
                avg("surface_temperature").alias("avg_surface_temp"),
                avg("sea_level_pressure").alias("avg_sea_level_pressure"),
                avg("wind_magnitude").alias("avg_wind_magnitude"),
                count("*").alias("record_count"),
            )
        )

    def calculate_climate_indices(self, df: DataFrame) -> DataFrame:
        """Calculate climate indices."""
        # Calculate CO2 anomaly (deviation from 400 ppm baseline)
        df = df.withColumn("co2_anomaly", col("avg_co2") - 400.0)

        # Calculate temperature anomaly (deviation from 288K baseline)
        df = df.withColumn("temp_anomaly", col("avg_surface_temp") - 288.0)

        return df

    def write_to_console(self, df: DataFrame) -> None:
        """Write processed data to console."""
        query = (
            df.writeStream.format("console")
            .outputMode("complete")
            .option("truncate", False)
            .start()
        )
        query.awaitTermination()

    def write_to_parquet(self, df: DataFrame, output_path: str) -> None:
        """Write processed data to Parquet files."""
        query = (
            df.writeStream.format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{self.checkpoint_location}/parquet")
            .outputMode("append")
            .start()
        )
        query.awaitTermination()


def run_atmospheric_processing_job(
    kafka_servers: str = "localhost:9092",
    output_path: str = os.environ.get(
        "ATMOSPHERIC_OUTPUT_DIR",
        os.path.join(tempfile.gettempdir(), "atmospheric_data"),
    ),
    mode: str = "batch",
) -> None:
    """Run the atmospheric data processing job.

    Args:
        kafka_servers: Kafka bootstrap servers
        output_path: Output path for processed data
        mode: "batch" or "streaming"
    """
    # Create Spark session
    ckpt_dir = os.path.join(tempfile.gettempdir(), "spark", "checkpoints")
    checkpoint_dir = os.environ.get("SPARK_CHECKPOINT_DIR", ckpt_dir)
    spark = (
        SparkSession.builder.appName("AtmosphericDataProcessing")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
        .getOrCreate()
    )

    processor = AtmosphericDataProcessor(spark, kafka_servers)

    # Read from Kafka
    raw_df = processor.read_from_kafka("batch.raw")

    # Parse atmospheric data
    atm_df = processor.parse_atmospheric_data(raw_df)

    # Filter for atmospheric data only
    atm_data = atm_df.filter(col("co2_concentration").isNotNull())

    if mode == "streaming":
        # Streaming mode: write to console
        processor.write_to_console(atm_data)
    else:
        # Batch mode: aggregate and write to parquet
        daily_agg = processor.aggregate_daily(atm_data)
        climate_indices = processor.calculate_climate_indices(daily_agg)
        processor.write_to_parquet(climate_indices, output_path)


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    run_atmospheric_processing_job(mode=mode)
