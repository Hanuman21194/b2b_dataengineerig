B2B Medallion Pipeline: Data Lineage, Schema Map, & Complete Setup Guide

This document serves as the master technical blueprint, operational reference, and schema map for the B2B Medallion Pipeline. It provides an end-to-end walkthrough of the project directory structure, dependencies, setup instructions, database lineage diagrams, and failover test strategies.

##  1. Project Directory Structure

Your B2B Medallion project workspace is organized using a production-grade layout:

b2b-medallion-pipeline/
├── ── data_generation/               
│   ├── generate_b2b_sales.py          # b2b sales executable script
│   └── generate_marketing_leads       # marketing seeds data generate script
    └── generate_weblogs               # weblogs generate script
├── requirements.txt                  # Python dependency list
├── test_connection.py                # JVM, OS, and Spark environment diagnostics script
├── ddl/                              # Database Initialization and Schema queries
│   ├── 01_init_schemas.sql           # Database schema creation scripts
│   └── 03_kpi_queries.sql            # Core SQL KPI reporting definitions
├── etl/                              # Python Execution Scripts
│   ├── main_pipeline.py              # Main Medallion pipeline (Bronze -> Silver -> Gold)
│   └── load_kpis.py                  # Standalone KPI calculation & reporting loader
    └── config.py                     #config parameters and placed
    └── load_weblogs.py               # executable script which load data into db
    └── db.py                         # db config values
    └── setup_ddl.py                  #all ddl create statements
    └──silver_layer.py                # all trabfirmed data ingested here
    └── gold_layer.py                 # clened and enriched data 
    └──bronze_layer.py                # all sources data 
    └──audit_trails.py                # audit tables data 


├                           
│── ├── data_lineage_and_schema_map.md# # Project documentation& Master schematic and project guide (this file)
│                                  
└── data/                             # File-system storage layer
    └── marketing_leads.csv           # Raw source marketing leads data file
    └── weblogs.log                   #weblogs generated data


## 2. System Prerequisites & Dependencies

To execute this PySpark-to-PostgreSQL pipeline, your local system must meet the following parameters:

Core Runtime Engines

Python: Version 3.10, 3.11, or 3.12 (Python 3.13 is supported using our configured Unix Socket Hotfix).

Java: Java Development Kit (JDK) 8, 11, or 17 (JDK 11 or 17 is highly recommended for optimal GC/JVM performance).

PostgreSQL: Version 12 or higher (configured with active schemas for source, bronze, silver, gold, and audit).

Python Dependencies (requirements.txt)

Create a requirements.txt file in your root folder with the following packages:

pyspark>=3.4.0,<3.5.0
psycopg2-binary>=2.9.0


## 3. Step-by-Step Installation & Run Procedure

Follow these instructions to initialize your local system and run the pipeline end-to-end.

Step 1: Initialize PostgreSQL Schemas & Tables

Before starting Spark, configure your PostgreSQL database. Connect using your preferred SQL client (e.g. pgAdmin, DBeaver, or psql) and run:

Create the database schemas:

CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;


Run your SQL schema initialization files (located in the ddl/ folder) to construct the source, staging, conformed reporting star schema, and auditing tables.

Step 2: Establish the Python Virtual Environment

Open a terminal inside your project's root folder and run:

# a. Create a clean virtual environment
python -m venv venv

# b. Activate the virtual environment
# On Windows (cmd):
venv\Scripts\activate
# On Windows (PowerShell):
venv\Scripts\Activate.ps1
# On macOS / Linux:
source venv/bin/activate

# c. Install core requirements
pip install -r requirements.txt


Step 3: Run Environmental Diagnostics

Verify that your OS environment variables, Java Virtual Machine (JVM) references, and PySpark-to-Java bridges are configured correctly:

python test_connection.py


Step 4: Run the Main Medallion Pipeline

Execute the full medallion pipeline:

python etl/main_pipeline.py


Step 5: Run Validation and KPI Reporting Analytics

Once your pipeline finishes successfully, compute conformed reporting tables and display live metrics dashboards:

python etl/load_kpis.py


## 4. End-to-End Data Flow Diagram

  [ PostgreSQL Source / CSV ]
               │
               ▼  (Phase 1: Full Extraction & Parallel Ingestion)
     ┌───────────────────┐
     │   BRONZE LAYER    │  <-- bronze.b2b_companies, b2b_orders, weblogs, etc.
     └─────────┬─────────┘
               │
               ▼  (Phase 2: Cleansing, Normalization, & DLQ Filtering)
     ┌───────────────────┐        ┌───────────────────────┐
     │   SILVER LAYER    │ ─────> │   AUDIT QUARANTINE    │ <-- audit.quarantine_records (JSONB format)
     └─────────┬─────────┘        └───────────────────────┘
               │
               ▼  (Phase 3: Kimball Star Schema Merge Upserts)
     ┌───────────────────┐
     │    GOLD LAYER     │  <-- gold.dim_*, gold.fact_sales, gold.fact_web_activity
     └───────────────────┘


##  5. Visual Database Schema Maps (PK / FK Relations)

Layer 1: Bronze Schema (Ingested Raw Source Data)

+-----------------------+            +-----------------------+            +-----------------------+
|   bronze.b2b_orders   |            | bronze.b2b_order_items|            |    bronze.weblogs     |
+-----------------------+            +-----------------------+            +-----------------------+
| [PK] order_id         | <--------+ | [PK] order_item_id    |            | [PK] log_id           |
| [FK] company_id       |            | [FK] order_id         |            |      client_ip        |
| [FK] customer_id      |            | [FK] product_id       |            |      username         |
|      order_timestamp  |            |      quantity         |            |      request_time     |
|      batch_id         |            |      unit_price       |            |      user_agent       |
+-----------------------+            |      batch_id         |            |      batch_id         |
                                     +-----------------------+            +-----------------------+

+-----------------------+            +-----------------------+            +-----------------------+
| bronze.b2b_companies  |            | bronze.b2b_end_cust   |            |  bronze.b2b_products  |
+-----------------------+            +-----------------------+            +-----------------------+
| [PK] company_id       |            | [PK] customer_id      |            | [PK] product_id       |
|      cuit             |            |      document_number  |            | [FK] supplier_id      |
|      company_name     |            |      full_name        |            |      product_name     |
|      is_supplier      |            |      date_of_birth    |            |      default_price    |
|      batch_id         |            |      batch_id         |            |      batch_id         |
+-----------------------+            +-----------------------+            +-----------------------+

+------------------------------------------------------------+
|                  bronze.marketing_leads                    |
+------------------------------------------------------------+
| [PK] lead_id                                               |
|      email                                                 |
|      company_name                                          |
|      lead_score                                            |
|      acquired_at                                           |
|      batch_id                                              |
+------------------------------------------------------------+


Layer 2: Silver Schema (Normalized & Cleansed Staging)

+-------------------------+          +-------------------------+          +-------------------------+
|    silver.companies     |          |    silver.customers     |          |     silver.products     |
+-------------------------+          +-------------------------+          +-------------------------+
| [PK] source_company_id  |          | [PK] source_customer_id |          | [PK] source_product_id  |
|      cuit               |          |      document_number    |          | [FK] source_supplier_id |
|      company_name       |          |      full_name          |          |      product_name       |
|      is_supplier        |          |      date_of_birth      |          |      default_price      |
|      batch_id           |          |      age (Calculated)   |          |      batch_id           |
+-------------------------+          |      batch_id           |          +-------------------------+
     ▲                               +-------------------------+               ▲
     │                                    ▲                                    │
     │ [FK] source_company_id             │ [FK] source_customer_id            │ [FK] source_product_id
     │                                    │                                    │
+────┴────────────────────────────────────┴────────────────────────────────────┴────────────────────+
|                                  silver.sales_order_lines                                         |
+---------------------------------------------------------------------------------------------------+
| [PK] source_order_item_id                                                                         |
|      source_order_id                                                                              |
| [FK] source_company_id   ========================================================================>|
| [FK] source_customer_id  ========================================================================>|
| [FK] source_product_id   ========================================================================>|
|      order_timestamp                                                                              |
|      quantity                                                                                     |
|      unit_price                                                                                   |
|      total_amount        ===> [Calculated: quantity * unit_price]                                 |
|      batch_id                                                                                     |
+---------------------------------------------------------------------------------------------------+

+---------------------------------------------------------------------------------------------------+
|                                     silver.web_activity                                           |
+---------------------------------------------------------------------------------------------------+
| [PK] source_log_id                                                                                |
|      client_ip                                                                                    |
|      username                                                                                     |
|      request_time                                                                                 |
|      user_agent                                                                                   |
| [FK] mapped_customer_id  ========================> Links to silver.customers.source_customer_id   |
|      device_type         ===> [Parsed from user_agent]                                            |
|      country             ===> [GeoIP lookup on client_ip]                                         |
|      city                ===> [GeoIP lookup on client_ip]                                         |
|      batch_id                                                                                     |
+---------------------------------------------------------------------------------------------------+

+---------------------------------------------------------------------------------------------------+
|                                    silver.marketing_leads                                         |
+---------------------------------------------------------------------------------------------------+
| [PK] lead_id                                                                                      |
|      email                                                                                        |
|      company_name                                                                                 |
|      lead_score                                                                                   |
|      acquired_at                                                                                  |
|      batch_id                                                                                     |
+---------------------------------------------------------------------------------------------------+


Layer 3: Gold Schema (Kimball Star Schema Reporting)

     +-------------------------+             +-------------------------+
     |     gold.dim_company    |             |     gold.dim_customer   |
     +-------------------------+             +-------------------------+
     | [PK] company_key        |             | [PK] customer_key       |
     | [UK] source_company_id  |             | [UK] source_customer_id |
     |      cuit               |             |      document_number    |
     |      company_name       |             |      full_name          |
     |      is_supplier        |             |      date_of_birth      |
     +------------┬------------+             |      age                |
                  │                          +------------┬------------+
                  │ (1 to many)                           │ (1 to many)
                  ▼                                       ▼
+───────────────────────────────────────────────────────────────────────────────+
|                               gold.fact_sales                                 |
+───────────────────────────────────────────────────────────────────────────────+
| [PK] source_order_item_id                                                     |
|      source_order_id                                                          |
| [FK] date_key             =================> Links to gold.dim_date (PK)      |
| [FK] company_key          =================> Links to gold.dim_company (PK)   |
| [FK] customer_key         =================> Links to gold.dim_customer (PK)  |
| [FK] product_key          =================> Links to gold.dim_product (PK)   |
|      quantity                                                                 |
|      unit_price                                                               |
|      total_amount                                                             |
|      etl_batch_id                                                             |
+───────────────────────────────────────────────────────────────────────────────+
                  ▲                                       ▲
                  │ (Many to 1)                           │ (Many to 1)
     +------------┴------------+             +------------┴------------+
     |     gold.dim_product    |             |       gold.dim_date     |
     +-------------------------+             +-------------------------+
     | [PK] product_key        |             | [PK] date_key           |
     | [UK] source_product_id  |             |      full_date          |
     |      source_supplier_id |             |      year / quarter     |
     |      product_name       |             |      month / month_name |
     |      default_price      |             |      day                |
     +-------------------------+             +-------------------------+



## 6. Operational Design & Failover Test Strategy

1. Robust Restartability Checkpoints (audit.pipeline_state)

The pipeline uses layer-level checkpointing for restartability. Before executing any phase, the pipeline reads the audit.pipeline_state table:

The Scenario: If a batch starts but fails during Phase 3 (Gold Layer), the pipeline logs a status of 'FAILED' for gold_star_schema_layer while keeping bronze_ingestion_layer and silver_cleansing_layer marked as 'SUCCESS'.

The Recovery Rerun: When you restart the job, it detects the failed batch, updates its batch status back to 'RUNNING', and resumes exactly from Phase 3, skipping Phase 1 (Bronze Ingestion) and Phase 2 (Silver Cleansing) entirely.

2. Standardized JSONB Quarantine (DLQ) Integration

PySpark writes values to PostgreSQL as VARCHAR strings by default. To safely save records to a native JSONB column without casting issues, we implement a Staging-Table-to-JSONB-Cast pattern:

PySpark writes raw JSON strings into a temporary text table:

CREATE TABLE IF NOT EXISTS audit.quarantine_records_staging (
    batch_id BIGINT,
    job_name VARCHAR,
    source_name VARCHAR,
    error_reason VARCHAR,
    raw_record TEXT
);


The staging table is truncated on each run. Spark writes to it using fast JDBC bulk inserts, and we cast the textual records into native JSONB in a single database transaction:

INSERT INTO audit.quarantine_records (batch_id, job_name, source_name, error_reason, raw_record)
SELECT batch_id, job_name, source_name, error_reason, raw_record::jsonb
FROM audit.quarantine_records_staging;


3. How to Test Failover & Recovery End-to-End

You can verify the restartability mechanism by simulating a crash:

Trigger a clean run:

python etl/main_pipeline.py


Induce a Crash:
Wait until you see Phase 2 (Silver Cleansing) run, then immediately press Ctrl + C to force-kill the python process.

Verify state in PostgreSQL:
Query your audit schema. You will see that audit.pipeline_state has marked bronze_ingestion_layer as 'SUCCESS' but silver_cleansing_layer as 'FAILED'.

Run the recovery job:

python etl/main_pipeline.py


Expected Result: The pipeline skips the Bronze ingestion layer entirely, reads the current batch records directly from the raw database tables, and completes successfully:

 Resuming Batch ID: 12 from silver layer.
 Skipping Bronze Ingestion Layer (Already successfully completed).
 Cleansing data for Silver Layer & Routing to DLQ...


## 9. Common Troubleshooting Guide

1. Windows UNIX Domain Sockets Error (UnixStreamServer)

Error Detail: AttributeError: module 'socketserver' has no attribute 'UnixStreamServer'

Root Cause: Python 3.13 on Windows exposes Unix Domain Sockets, but the socketserver library inside older PySpark versions does not check for OS compatibility, causing a crash during execution.

Resolution: Our pipeline includes an active hotfix that dynamically defines a dummy class wrapper on Windows:

if sys.platform == "win32" and not hasattr(socketserver, "UnixStreamServer"):
    class _DummyUnixStreamServer: pass
    socketserver.UnixStreamServer = _DummyUnixStreamServer


2. JVM Heap Crash or GC Overhead Limit Exceeded

Error Detail: java.lang.OutOfMemoryError: GC over limit

Root Cause: Spark driver running on localhost lacks sufficient RAM allocations.

Resolution: Increase local Spark driver memory bounds prior to session initialization by setting the following configuration parameter:

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


3. Class Mismatch Conflict (org.postgresql.Driver not found)

Error Detail: java.lang.ClassNotFoundException: org.postgresql.Driver

Root Cause: The PostgreSQL JDBC Driver coordinate was not loaded by Spark's underlying class loader.

Resolution: Ensure your environment has an active internet connection on the first run so that Spark can dynamically download the PostgreSQL driver JAR. Additionally, clear any lingering custom SPARK_HOME or HADOOP_HOME variables in your Windows Control Panel to prevent conflicts.