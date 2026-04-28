# Engineering Data Platform - User Documentation

Welcome to the **Engineering Data Platform** documentation. This platform provides a comprehensive data engineering solution for ingesting, orchestrating, and validating NREL wind simulation and NASA satellite data.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Configuration](#configuration)
4. [Running the Platform](#running-the-platform)
5. [Data Quality](#data-quality)
6. [Monitoring](#monitoring)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker | 20.10+ | Container runtime |
| Docker Compose | 2.0+ | Container orchestration |
| Python | 3.12+ | Development |
| Kubernetes (optional) | 1.28+ | Production deployment |

### 5-Minute Setup

```bash
# 1. Clone the repository
git clone https://github.com/your-org/engi-stream-core.git
cd engi-stream-core

# 2. Configure environment
cp data-ingestion/.env.example data-ingestion/.env

# 3. Start with Docker Compose
cd data-ingestion/containerization/docker
docker-compose up -d

# 4. Verify services
docker-compose ps
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Engineering Data Platform                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │   Ingestion  │───▶│    Kafka     │───▶│    Spark     │───▶│ PostgreSQL│ │
│  │   Service    │    │  Message Bus │    │  Processing  │    │  Database │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘ │
│         │                   │                   │                   │       │
│         ▼                   ▼                   ▼                   ▼       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │ NREL/NASA    │    │  Airflow      │    │    Great     │    │  MinIO   │ │
│  │ API Clients  │    │  Orchestration│   │ Expectations │    │  (S3)   │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description | Port |
|-----------|-------------|------|
| **Ingestion Service** | Fetches NREL wind and NASA satellite data | 8000 |
| **Kafka** | Message broker for real-time and batch data | 9092 |
| **Airflow** | Workflow orchestration | 8080 |
| **Spark** | Data processing engine | 8081 |
| **PostgreSQL** | Data warehouse | 5432 |
| **MinIO** | S3-compatible storage | 9000 |

---

## Configuration

### Environment Variables

Create `data-ingestion/.env`:

```bash
# API Keys
NREL_API_KEY=your_nrel_api_key
NASA_API_KEY=DEMO_KEY

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/energy_data
```

### Getting API Keys

| API | URL | Notes |
|-----|-----|-------|
| **NREL** | https://developer.nrel.gov/ | Free registration required |
| **NASA** | https://api.nasa.gov/ | Use `DEMO_KEY` for testing |

---

## Running the Platform

### Development (Docker Compose)

```bash
cd data-ingestion/containerization/docker

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f ingestion-service

# Stop services
docker-compose down
```

### Production (Kubernetes)

```bash
# Using Helm
cd data-ingestion/containerization/helm/data-platform
helm install data-platform . --namespace data-platform

# Or using kubectl
kubectl apply -f ../../kubernetes/base/
```

### Local Development (Python)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r data-ingestion/requirements.txt

# Run ingestion service
cd data-ingestion/services/ingestion/src
python -m app
```

---

## Data Quality

### Engineering Unit Bounds

The platform validates all data against realistic engineering bounds:

| Parameter | Minimum | Maximum | Unit |
|-----------|---------|---------|------|
| Wind Speed | 0 | 50 | m/s |
| Wind Direction | 0 | 360 | degrees |
| Temperature | -50 | 60 | °C |
| Pressure | 80,000 | 110,000 | Pa |
| Air Density | 0.9 | 1.5 | kg/m³ |
| CO2 Concentration | 300 | 600 | ppm |
| CH4 Concentration | 1,500 | 2,500 | ppb |
| Surface Temperature | 200 | 350 | K |

### Running Quality Checks

```bash
# Run Great Expectations validation
docker exec -it great-expectations gx checkpoint run energy_data_checkpoint

# Or via Python
cd data-ingestion/quality-control
python -c "from validators.energy_data_validator import run_validation; print(run_validation())"
```

---

## Monitoring

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin/admin |
| Spark UI | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| PostgreSQL | localhost:5432 | postgres/postgres |

### Health Checks

```bash
# Check all containers
docker-compose ps

# Check specific service logs
docker-compose logs airflow-webserver

# Check Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

## Troubleshooting

### Common Issues

#### 1. Kafka Connection Failed

```bash
# Check Kafka status
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

#### 2. Airflow DAG Not Running

```bash
# Clear DAG import errors
docker exec -it airflow-webserver airflow dags reset --dag_id daily_batch_ingestion

# Trigger manual run
docker exec -it airflow-webserver airflow dags trigger daily_batch_ingestion
```

#### 3. Data Quality Validation Failed

```bash
# View validation results
docker exec -it great-expectations gx checkpoint run energy_data_checkpoint --verbose

# Check bounds in expectations.py
cat data-ingestion/quality-control/great_expectations/expectations.py
```

#### 4. API Rate Limits

```bash
# Use cached data for testing
# Edit .env to use DEMO_KEY for NASA
NASA_API_KEY=DEMO_KEY
```

### Logs Location

| Service | Log Path |
|---------|----------|
| Ingestion | `docker-compose logs ingestion-service` |
| Airflow | `docker-compose logs airflow-webserver` |
| Spark | `docker-compose logs spark-master` |
| Kafka | `docker-compose logs kafka` |

---

## API Reference

### Ingestion Service Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ingest/wind` | POST | Trigger wind data ingestion |
| `/ingest/satellite` | POST | Trigger satellite data ingestion |
| `/status` | GET | Get ingestion status |

### Example Usage

```bash
# Trigger wind data ingestion
curl -X POST http://localhost:8000/ingest/wind \
  -H "Content-Type: application/json" \
  -d '{"lat": 39.7406, "lon": -104.9917, "hub_height": 80}'

# Check status
curl http://localhost:8000/status
```

---

## Next Steps

- Review the [Architecture Documentation](../AGENTS.md)
- Explore the [Configuration Guide](./configuration.md)
- Set up [Monitoring](./monitoring.md)
- Learn about [Security](./security.md)

---

## Support

- **Issues**: https://github.com/your-org/engi-stream-core/issues
- **Documentation**: https://docs.example.com
- **Slack**: #data-engineering-platform