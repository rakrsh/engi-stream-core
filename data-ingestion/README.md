# Data Engineering Platform

A comprehensive data engineering platform that pulls NREL Wind Simulation data and NASA Satellite data, orchestrates batch and real-time processing using Apache Airflow and Apache Kafka, containerizes the stack for local Kubernetes deployment, and implements data quality validation using Great Expectations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Data Engineering Platform                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │   Ingestion     │     │  Orchestration  │     │ Quality Control│       │
│  │   Service       │     │     Layer       │     │     Layer      │       │
│  ├─────────────────┤     ├─────────────────┤     ├─────────────────┤       │
│  │ • NREL Fetcher  │────▶│ • Airflow DAGs  │────▶│ Great Expectations│    │
│  │ • NASA Fetcher  │     │ • Kafka Producer│     │ • Validators   │       │
│  │ • Batch Mode    │     │ • Kafka Consumer│     │ • Checkpoints  │       │
│  │ • Real-time     │     │ • Spark Jobs    │     │ • Bounds Check │       │
│  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘       │
│           │                       │                       │                 │
│           ▼                       ▼                       ▼                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        Kafka Message Bus                            │    │
│  │  ┌────────────────────┐  ┌────────────────────┐                   │    │
│  │  │  telemetry.raw    │  │    batch.raw        │                   │    │
│  │  │  (Real-time)      │  │  (Batch)            │                   │    │
│  │  └────────────────────┘  └────────────────────┘                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Ingestion Service (`services/ingestion/`)
- **NREL Wind Fetcher**: Fetches wind simulation data from NREL API
- **NASA Satellite Fetcher**: Fetches satellite imagery and atmospheric data
- Supports both batch and real-time ingestion modes

### 2. Orchestration (`services/orchestration/`)
- **Airflow DAGs**: 
  - `daily_batch_ingestion.py` - Daily batch data load
  - `realtime_telemetry.py` - Real-time telemetry processing
- **Kafka Producers/Consumers**: For message streaming
- **Spark Jobs**: For data processing and aggregation

### 3. Containerization (`containerization/`)
- **Docker**: Individual service Dockerfiles
- **Docker Compose**: Local development stack
- **Kubernetes**: Base manifests for K8s deployment
- **Helm**: Chart for easy K8s deployment

### 4. Quality Control (`quality-control/`)
- **Great Expectations**: Data quality validation
- **Validators**: Engineering unit bounds checking
- **Checkpoints**: Pre-defined validation suites

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+
- Kubernetes (Minikube or Kind) - for K8s deployment

### Local Development with Docker Compose

```bash
# Navigate to docker directory
cd data-ingestion/containerization/docker

# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### Deploy to Kubernetes

```bash
# Using Helm
cd data-ingestion/containerization/helm/data-platform

# Install chart
helm install data-platform . --namespace data-platform

# Or apply manifests directly
kubectl apply -f ../kubernetes/base/
```

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp data-ingestion/.env.example data-ingestion/.env
```

Required environment variables:
- `NREL_API_KEY` - NREL API key
- `NASA_API_KEY` - NASA API key (default: DEMO_KEY)
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka server address
- `DATABASE_URL` - PostgreSQL connection string

## Data Quality Bounds

The platform validates engineering units against realistic bounds:

| Parameter | Min | Max | Unit |
|-----------|-----|-----|------|
| Wind Speed | 0 | 50 | m/s |
| Wind Direction | 0 | 360 | degrees |
| Temperature | -50 | 60 | °C |
| Pressure | 80,000 | 110,000 | Pa |
| CO2 Concentration | 300 | 600 | ppm |
| Surface Temperature | 200 | 350 | K |

## Project Structure

```
data-ingestion/
├── services/
│   ├── ingestion/
│   │   ├── src/
│   │   │   ├── app.py              # Main entry point
│   │   │   ├── config/             # Configuration
│   │   │   └── modules/            # Data fetchers
│   │   └── requirements.txt
│   └── orchestration/
│       ├── airflow/dags/           # Airflow DAGs
│       ├── kafka/
│       │   ├── producers/          # Kafka producers
│       │   └── consumers/          # Kafka consumers
│       └── spark/jobs/             # Spark jobs
├── containerization/
│   ├── docker/                     # Dockerfiles & compose
│   ├── kubernetes/                 # K8s manifests
│   └── helm/                       # Helm charts
├── quality-control/
│   ├── great_expectations/         # GX expectations
│   └── validators/                 # Custom validators
└── .env.example                    # Environment template
```

## Monitoring

- Airflow UI: http://localhost:8080
- Spark UI: http://localhost:8081
- Kafka: localhost:9092
- PostgreSQL: localhost:5432

## License

MIT License - See LICENSE file for details