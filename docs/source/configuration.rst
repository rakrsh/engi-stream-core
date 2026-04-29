##############################################################################
Configuration Guide
##############################################################################

This guide covers all configuration options for the Engineering Data Platform.

================================================================================
Environment Variables
================================================================================

Core Configuration
==================

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``ENV``
     - ``development``
     - Environment mode (``development``, ``production``)
   * - ``LOG_LEVEL``
     - ``INFO``
     - Logging level (``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``)

API Keys
========

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``NREL_API_KEY``
     - -
     - NREL API key for wind data
   * - ``NASA_API_KEY``
     - ``DEMO_KEY``
     - NASA API key for satellite data

Kafka Configuration
===================

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``KAFKA_BOOTSTRAP_SERVERS``
     - ``localhost:9092``
     - Kafka broker addresses
   * - ``KAFKA_TOPIC_TELEMETRY``
     - ``telemetry.raw``
     - Real-time telemetry topic
   * - ``KAFKA_TOPIC_BATCH``
     - ``batch.raw``
     - Batch data topic
   * - ``KAFKA_CONSUMER_GROUP``
     - ``ingestion-service``
     - Consumer group ID

Database Configuration
======================

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``DATABASE_URL``
     - ``postgresql://postgres:postgres@localhost:5432/energy_data``
     - PostgreSQL connection string

Service Configuration
=====================

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``BATCH_SIZE``
     - ``1000``
     - Records per batch
   * - ``REQUEST_TIMEOUT``
     - ``30``
     - HTTP request timeout (seconds)
   * - ``MAX_RETRIES``
     - ``3``
     - Maximum retry attempts
   * - ``RETRY_DELAY``
     - ``5``
     - Retry delay (seconds)

Great Expectations
==================

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``GREAT_EXPECTATIONS_CHECKPOINT``
     - ``energy_data_checkpoint``
     - GX checkpoint name
   * - ``GX_HOME``
     - ``/tmp/gx``
     - Great Expectations home directory

================================================================================
Configuration Files
================================================================================

Ingestion Service
=================

Location: ``data-ingestion/services/ingestion/src/config/settings.py``

.. code-block:: python

   from pydantic_settings import BaseSettings

   class IngestionSettings(BaseSettings):
       nrel_api_key: str = ""
       nasa_api_key: str = "DEMO_KEY"
       kafka_bootstrap_servers: str = "localhost:9092"
       database_url: str = "postgresql://postgres:postgres@localhost:5432/energy_data"
       # ... more settings

Docker Compose
==============

Location: ``data-ingestion/containerization/docker/docker-compose.yaml``

Key services:

* ``zookeeper`` - Kafka coordination
* ``kafka`` - Message broker
* ``postgres`` - Data warehouse
* ``airflow-webserver`` - Workflow UI
* ``airflow-scheduler`` - Workflow scheduler
* ``spark-master`` - Spark master
* ``spark-worker`` - Spark workers
* ``ingestion-service`` - Data ingestion
* ``great-expectations`` - Data quality
* ``minio`` - S3 storage

Kubernetes
==========

Location: ``data-ingestion/containerization/kubernetes/base/``

Key manifests:

* ``namespace.yaml`` - Namespace and secrets
* ``ingestion-deployment.yaml`` - Ingestion service
* ``kafka-deployment.yaml`` - Kafka and Zookeeper
* ``airflow-deployment.yaml`` - Airflow webserver and scheduler
* ``spark-deployment.yaml`` - Spark master and workers
* ``postgres-deployment.yaml`` - PostgreSQL database
