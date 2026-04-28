# Configuration Guide

This guide covers all configuration options for the Engineering Data Platform.

---

## Environment Variables

### Core Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ENV` | `development` | Environment mode (`development`, `production`) |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

### API Keys

| Variable | Default | Description |
|----------|---------|-------------|
| `NREL_API_KEY` | - | NREL API key for wind data |
| `NASA_API_KEY` | `DEMO_KEY` | NASA API key for satellite data |

### Kafka Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC_TELEMETRY` | `telemetry.raw` | Real-time telemetry topic |
| `KAFKA_TOPIC_BATCH` | `batch.raw` | Batch data topic |
| `KAFKA_CONSUMER_GROUP` | `ingestion-service` | Consumer group ID |

### Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/energy_data` | PostgreSQL connection string |

### Service Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | `1000` | Records per batch |
| `REQUEST_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `MAX_RETRIES` | `3` | Maximum retry attempts |
| `RETRY_DELAY` | `5` | Retry delay (seconds) |

### Great Expectations

| Variable | Default | Description |
|----------|---------|-------------|
| `GREAT_EXPECTATIONS_CHECKPOINT` | `energy_data_checkpoint` | GX checkpoint name |
| `GX_HOME` | `/tmp/gx` | Great Expectations home directory |

---

## Configuration Files

### Ingestion Service

Location: `data-ingestion/services/ingestion/src/config/settings.py`

```python
from pydantic_settings import BaseSettings

class IngestionSettings(BaseSettings):
    nrel_api_key: str = ""
    nasa_api_key: str = "DEMO_KEY"
    kafka_bootstrap_servers: str = "localhost:9092"
    database_url: str = "postgresql://postgres:postgres@localhost:5432/energy_data"
    # ... more settings
```

### Docker Compose

Location: `data-ingestion/containerization/docker/docker-compose.yaml`

Key services:
- `zookeeper` - Kafka coordination
- `kafka` - Message broker
- `postgres` - Data warehouse
- `airflow-webserver` - Workflow UI
- `airflow-scheduler` - Workflow scheduler
- `spark-master` - Spark master
- `spark-worker` - Spark workers
- `ingestion-service` - Data ingestion
- `great-expectations` - Data quality
- `minio` - S3 storage

### Kubernetes

Location: `data-ingestion/containerization/kubernetes/base/`

Key manifests:
- `namespace.yaml` - Namespace and secrets
- `ingestion-deployment.yaml` - Ingestion service
- `kafka-deployment.yaml` - Kafka and Zookeeper
- `airflow-deployment.yaml` - Airflow webserver and scheduler
- `spark-deployment.yaml` - Spark master and workers
- `postgres-deployment.yaml` - PostgreSQL database

### Helm Values

Location: `data-ingestion/containerization/helm/data-platform/values.yaml`

```yaml
ingestion:
  replicaCount: 2
  image:
    repository: ingestion-service
    tag: latest
  config:
    kafka:
      bootstrapServers: "kafka:29092"

airflow:
  replicaCount: 1
  config:
    executor: LocalExecutor

kafka:
  replicaCount: 1
  persistence:
    enabled: true
    size: 20Gi
```

---

## Topic Configuration

### Kafka Topics

| Topic | Type | Partitions | Retention |
|-------|------|------------|-----------|
| `telemetry.raw` | Real-time | 3 | 7 days |
| `batch.raw` | Batch | 1 | 30 days |

### Creating Topics

```bash
# Using kafka-topics
docker exec -it kafka kafka-topics \
  --create \
  --topic telemetry.raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## Database Schema

### Tables

```sql
-- Wind data table
CREATE TABLE wind_data (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(255),
    timestamp TIMESTAMP,
    wind_speed FLOAT,
    wind_direction FLOAT,
    temperature FLOAT,
    pressure FLOAT,
    location_lat FLOAT,
    location_lon FLOAT,
    fetched_at TIMESTAMP
);

-- Atmospheric data table
CREATE TABLE atmospheric_data (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(255),
    timestamp TIMESTAMP,
    co2_concentration FLOAT,
    ch4_concentration FLOAT,
    surface_temperature FLOAT,
    sea_level_pressure FLOAT,
    location_lat FLOAT,
    location_lon FLOAT,
    fetched_at TIMESTAMP
);

-- Daily metrics table
CREATE TABLE daily_metrics (
    id SERIAL PRIMARY KEY,
    date DATE,
    source VARCHAR(50),
    record_count INTEGER,
    created_at TIMESTAMP
);
```

---

## Airflow DAG Configuration

### Daily Batch DAG

Location: `data-ingestion/services/orchestration/airflow/dags/daily_batch_ingestion.py`

```python
with DAG(
    dag_id="daily_batch_ingestion",
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2024, 1, 1),
) as dag:
    # Tasks: extraction -> quality -> processing -> notification
```

### Real-time Telemetry DAG

Location: `data-ingestion/services/orchestration/airflow/dags/realtime_telemetry.py`

```python
with DAG(
    dag_id="realtime_telemetry",
    schedule_interval=timedelta(minutes=1),  # Every minute
) as dag:
    # Tasks: kafka consumption -> processing -> alerts
```

---

## Spark Configuration

### Wind Processor Job

Location: `data-ingestion/services/orchestration/spark/jobs/wind_processor.py`

```python
SparkSession.builder \
    .appName("WindDataProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
```

### Atmospheric Processor Job

Location: `data-ingestion/services/orchestration/spark/jobs/atmospheric_processor.py`

```python
SparkSession.builder \
    .appName("AtmosphericDataProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
```

---

## Security Configuration

### API Keys

Never commit API keys to version control. Use environment variables:

```bash
# .env (add to .gitignore)
NREL_API_KEY=your_key_here
NASA_API_KEY=your_key_here
```

### Database Credentials

```bash
# Use strong passwords in production
POSTGRES_PASSWORD=generate_strong_password
```

### Network Policies

Enable in `values.yaml`:

```yaml
networkPolicy:
  enabled: true
  ingress:
    enabled: true
  egress:
    enabled: true
```