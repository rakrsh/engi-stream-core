# Engi-Stream Core

[![CI/CD Pipeline](https://github.com/rakrsh/engi-stream-core/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/rakrsh/engi-stream-core/actions/workflows/ci-cd.yml)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance, agentic data engineering platform designed to pull, orchestrate, and validate complex data streams (NREL Wind Simulation, NASA Satellite data, etc.). It leverages Apache Airflow, Apache Kafka, and Great Expectations for robust data lifecycles.

## 🚀 Key Features

- **Agentic Architecture**: Decentralized agents for ingestion and orchestration.
- **Multi-Source Ingestion**: Support for NREL API, NASA Satellite data, and real-time streams.
- **Robust Orchestration**: Batch processing with Airflow and real-time streaming with Kafka/Spark.
- **Automated Quality Control**: Built-in data validation using Great Expectations.
- **Cloud Native**: Containerized with Docker and ready for Kubernetes with Helm charts.
- **CI/CD Driven**: Full pipeline for security checks, automated tests, and containerized deployments.

## 🏗️ Architecture

Engi-Stream Core utilizes a decentralized agentic model:

- **Ingestion Agent**: Pulls data from external APIs and streams.
- **Orchestration Agent**: Coordinates data flow between ingestion and downstream consumers.
- **Quality Control Layer**: Validates incoming data against engineering bounds.

Refer to [AGENTS.md](AGENTS.md) for detailed agent specifications.

## 📁 Project Structure

```text
.
├── .github/workflows/      # CI/CD Pipeline configurations
├── AGENTS.md               # Detailed Agent Architecture
├── docs/                   # Documentation site (Sphinx)
├── data-ingestion/         # Core application logic
│   ├── services/
│   │   ├── ingestion/      # Ingestion logic (NREL/NASA fetchers)
│   │   └── orchestration/  # Airflow DAGs, Kafka, and Spark jobs
│   ├── containerization/
│   │   ├── docker/         # Service Dockerfiles & Compose
│   │   ├── kubernetes/     # Kubernetes manifests
│   │   └── helm/           # Helm charts for deployment
│   └── quality-control/    # Great Expectations validation suites
└── README.md
```

## 🛠️ Getting Started

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- Kubernetes (Minikube/Kind) for Helm deployments
- [Pre-commit](https://pre-commit.com/) installed

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/rakrsh/engi-stream-core.git
   cd engi-stream-core
   ```

2. **Set up local environment**:
   ```bash
   cp data-ingestion/.env.example data-ingestion/.env
   # Configure your API keys in .env
   ```

3. **Install Pre-commit hooks**:
   ```bash
   pre-commit install
   ```

### Running Locally

```bash
cd data-ingestion/containerization/docker
docker-compose up -d
```

## 🧪 Development

### Running Tests
```bash
pytest
```

### Manual Linting
```bash
pre-commit run --all-files
```

## 🚢 CI/CD & Deployment

The project uses GitHub Actions for a comprehensive CI/CD pipeline:
- **Security**: SAST with `bandit` and SCA with `safety`.
- **License Compliance**: Automated checks with `pip-licenses`.
- **Docker**: Automatically builds and pushes images to GHCR.
- **Helm**: Packages and pushes charts to the GitHub registry.
- **Docs**: Automatically builds and deploys the documentation site using Sphinx.

## 📖 Documentation

Full documentation is available in the `docs/` folder and is automatically hosted via GitHub Pages. To view locally:
```bash
pip install -r docs/requirements.txt
sphinx-build -b html docs/source docs/build/html
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
