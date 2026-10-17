\# Real-Time E-Commerce Data Engineering Platform



An end-to-end data engineering project that simulates e-commerce events, processes streaming data with Apache Kafka and PySpark, stores raw data in AWS S3, and delivers analytical datasets to Snowflake using Apache Airflow orchestration.



\## Project Overview



This project demonstrates how a modern data engineering pipeline can ingest, process, validate, store, and analyze e-commerce event data.



The platform generates simulated events such as order creation, payment completion, payment failure, order cancellation, and inventory updates. Kafka handles event ingestion, PySpark processes streaming events, AWS S3 stores raw data, and Snowflake serves as the analytics warehouse.



Apache Airflow orchestrates the downstream data pipeline, including data quality validation, Gold-layer analytics, and Snowflake loading.



\## Architecture



```text

E-Commerce Event Generator (Python)

&#x20;                |

&#x20;                v

&#x20;      Apache Kafka Topic

&#x20;      ecommerce\_events

&#x20;                |

&#x20;                v

&#x20;   PySpark Structured Streaming

&#x20;                |

&#x20;         +------+------+

&#x20;         |             |

&#x20;         v             v

&#x20;    Bronze Layer    Silver Layer

&#x20;    Raw Events      Clean Events

&#x20;         |             |

&#x20;         v             v

&#x20;       AWS S3       Payment Metrics

&#x20;         |

&#x20;         v

&#x20;     Apache Airflow

&#x20;         |

&#x20;         v

&#x20;  Data Quality Validation

&#x20;         |

&#x20;         v

&#x20;    Gold Analytics

&#x20;         |

&#x20;         v

&#x20;      Snowflake

&#x20;         |

&#x20;         v

&#x20;   SQL Analytics

```



\*Note: The diagram represents the logical data flow. The exact execution order and dependencies are defined in the project's code and Airflow DAG.\*



\## Technology Stack



| Technology and Purpose

|-----------------------------

| Python - Event generation, pipeline utilities, and data processing

| Apache Kafka - Streaming event ingestion and buffering

| PySpark Structured Streaming - Stream processing, parsing, validation, and aggregation

| Apache Airflow - Workflow orchestration and task monitoring

| AWS S3 - Cloud storage for raw data and Parquet files

| Snowflake - Cloud data warehouse for analytical datasets

| Docker Compose - Local containerized services

| SQL - Data validation and analytical queries

| Git and GitHub - Version control and source code management



\## Data Pipeline



\### 1. Event Generation

A Python producer generates simulated e-commerce events and publishes them to the `ecommerce\_events` Kafka topic.



\### 2. Kafka Ingestion

Kafka receives and buffers incoming events across topic partitions, enabling downstream streaming consumption.



\### 3. PySpark Streaming

PySpark Structured Streaming reads Kafka events, parses JSON payloads, applies schema-based validation, and processes valid and invalid records.



The pipeline writes event data to Bronze and Silver datasets and generates payment-related aggregations.



\### 4. Bronze and Silver Layers

\- \*\*Bronze:\*\* Stores raw Kafka event data for downstream processing and analysis.

\- \*\*Silver:\*\* Stores cleaned and validated event data in Parquet format.



\### 5. AWS S3 Storage

The S3 uploader transfers eligible Bronze Parquet files to an S3 bucket. Existing files can be skipped to reduce unnecessary uploads.



\### 6. Data Quality Validation

The pipeline validates data quality using checks such as schema and required-column validation, null checks, payment amount and count validation, and source-to-target reconciliation where applicable.



\### 7. Gold Analytics

The Gold layer produces analytical datasets such as:

\- Payment metrics

\- City-level payment metrics

\- Event-type metrics

\- Product metrics



\### 8. Snowflake Loading

The pipeline loads analytical datasets into Snowflake tables for SQL-based reporting and analysis.



\### 9. Airflow Orchestration

The `ecommerce\_pipeline` DAG coordinates downstream tasks, including processed-data checks, S3 uploads, data quality validation, Gold analytics, Snowflake loading, and analytics.



\## Repository Structure



```text

Real\_Time\_Ecommerce\_Platform/

├── dags/

│   └── ecommerce\_pipeline.py

├── docker/

│   └── docker-compose.yml

├── src/

│   ├── event\_generator.py

│   ├── kafka\_stream.py

│   ├── data\_quality.py

│   ├── gold\_analytics.py

│   ├── analytics.py

│   ├── s3\_bronze\_uploader.py

│   ├── snowflake\_loader.py

│   └── spark\_test.py

├── .gitignore

└── README.md

```



Runtime directories, virtual environments, generated data, checkpoints, and local configuration files are intentionally excluded from version control where appropriate.



\## Prerequisites



\- Python

\- Java compatible with the configured Spark environment

\- Apache Kafka

\- Apache Spark / PySpark

\- Docker Desktop with Docker Compose

\- AWS account and an appropriately configured S3 bucket

\- Snowflake account, warehouse, database, schema, and required permissions



Exact versions and configuration requirements may vary by environment.



\## Setup and Execution



\### 1. Clone the repository



```bash

git clone <YOUR\_GITHUB\_REPOSITORY\_URL>

cd Real\_Time\_Ecommerce\_Platform

```



\### 2. Configure the environment



Create a Python virtual environment and install the dependencies required by the project.



Configure AWS credentials and Snowflake connection details using environment variables or an approved secrets-management mechanism. Do not commit credentials or `.env` files.



\### 3. Start Kafka



From the project root, start the Kafka service using the included Docker Compose file:



```bash

docker compose -f docker/docker-compose.yml up -d

```



Confirm that the Kafka service is healthy and that the required topic exists before starting the producer.



\### 4. Generate events



Run the event generator from the project root:



```bash

python src/event\_generator.py

```



\### 5. Start the PySpark streaming job



In a separate terminal, start the streaming processor:



```bash

python src/kafka\_stream.py

```



Ensure the configured Kafka bootstrap server, Spark dependencies, checkpoint paths, and Python environment are available.



\### 6. Run the downstream pipeline



Configure the Airflow environment and its connections, then trigger the `ecommerce\_pipeline` DAG from the Airflow UI or through the supported CLI.



Verify the S3 objects, data quality results, Gold datasets, and Snowflake tables after execution.



\*These commands provide a starting point. Use the project's actual dependency files, environment settings, and service configuration when available.\*



\## Data Quality and Validation



The project emphasizes data correctness through:

\- Schema and required-column checks

\- Null and invalid-value validation

\- Payment metric validation

\- Parquet dataset checks

\- Source-to-target record and aggregate reconciliation where applicable

\- Monitoring of downstream Airflow task results



\## Key Learning Outcomes



\- Building a streaming ingestion pipeline with Apache Kafka

\- Processing JSON events using PySpark Structured Streaming

\- Working with Bronze, Silver, and Gold data layers

\- Writing Parquet datasets and managing streaming checkpoints

\- Integrating AWS S3 with a data engineering workflow

\- Orchestrating data pipelines using Apache Airflow

\- Loading analytical datasets into Snowflake

\- Performing data quality validation and SQL reconciliation

\- Running supporting services with Docker Compose



\## Security



\- Do not commit AWS access keys, Snowflake passwords, private keys, or other secrets.

\- Keep credentials outside source control.

\- Use least-privilege permissions for AWS and Snowflake.

\- Review the repository and Git history before making the project public.



\## Author



\*\*Amit Kumar Chaudhary\*\*



Data Engineering | Python | SQL | Apache Kafka | PySpark | Apache Airflow | AWS S3 | Snowflake
