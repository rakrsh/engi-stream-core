##############################################################################
Engineering Data Platform - User Documentation
##############################################################################

Welcome to the **Engineering Data Platform** documentation. This platform provides a comprehensive data engineering solution for ingesting, orchestrating, and validating NREL wind simulation and NASA satellite data.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   configuration
   data-quality
   agents

================================================================================
Quick Start
================================================================================

Prerequisites
=============

.. list-table::
   :header-rows: 1

   * - Tool
     - Version
     - Purpose
   * - Docker
     - 20.10+
     - Container runtime
   * - Docker Compose
     - 2.0+
     - Container orchestration
   * - Python
     - 3.12+
     - Development
   * - Kubernetes (optional)
     - 1.28+
     - Production deployment

5-Minute Setup
==============

.. code-block:: bash

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

================================================================================
Architecture Overview
================================================================================

Components
==========

.. list-table::
   :header-rows: 1

   * - Component
     - Description
     - Port
   * - Ingestion Service
     - Fetches NREL wind and NASA satellite data
     - 8000
   * - Kafka
     - Message broker for real-time and batch data
     - 9092
   * - Airflow
     - Workflow orchestration
     - 8080
   * - Spark
     - Data processing engine
     - 8081
   * - PostgreSQL
     - Data warehouse
     - 5432
   * - MinIO
     - S3-compatible storage
     - 9000

================================================================================
Configuration
================================================================================

Environment Variables
======================

Create ``data-ingestion/.env``:

.. code-block:: bash

   # API Keys
   NREL_API_KEY=your_nrel_api_key
   NASA_API_KEY=DEMO_KEY

   # Kafka
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092

   # Database
   DATABASE_URL=postgresql://postgres:postgres@localhost:5432/energy_data

Getting API Keys
================

.. list-table::
   :header-rows: 1

   * - API
     - URL
     - Notes
   * - NREL
     - https://developer.nrel.gov/
     - Free registration required
   * - NASA
     - https://api.nasa.gov/
     - Use ``DEMO_KEY`` for testing

================================================================================
Running the Platform
================================================================================

Development (Docker Compose)
=============================

.. code-block:: bash

   cd data-ingestion/containerization/docker

   # Start all services
   docker-compose up -d

   # View logs
   docker-compose logs -f ingestion-service

   # Stop services
   docker-compose down

Production (Kubernetes)
========================

.. code-block:: bash

   # Using Helm
   cd data-ingestion/containerization/helm/data-platform
   helm install data-platform . --namespace data-platform

   # Or using kubectl
   kubectl apply -f ../../kubernetes/base/

Local Development (Python)
==========================

.. code-block:: bash

   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or: venv\Scripts\activate  # Windows

   # Install dependencies
   pip install -r data-ingestion/requirements.txt

   # Run ingestion service
   cd data-ingestion/services/ingestion/src
   python -m app
