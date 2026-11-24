# End-to-End Car Sales Data Engineering Pipeline (Azure & Databricks)

An enterprise-grade batch data engineering pipeline that ingests incremental car sales data from Azure SQL Database, processes it across a Medallion Architecture in Azure Data Lake Storage Gen2 (ADLS Gen2), and models a Star Schema with Slowly Changing Dimensions (SCD Type 2) in Azure Databricks.

## Architecture Overview
![Architecture Diagram](assets/architecture_diagram.png)

## Tech Stack
* **Cloud Platform:** Microsoft Azure
* **Source Database:** Azure SQL Database
* **Orchestration & Ingestion:** Azure Data Factory (ADF)
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Processing & Transformation:** Azure Databricks (PySpark, Spark SQL, Delta Lake)
* **Governance:** Unity Catalog
* **CI/CD & Version Control:** GitHub

## Repository Structure
├── adf/
│   ├── pipeline/
│   │   ├── increm_data_pipeline.json      # Main watermark ingestion pipeline
│   │   └── source_prep.json               # Initial historical data load pipeline
│   └── dataset/                           # Parameterized Azure SQL & ADLS datasets
├── sql/
│   ├── create_watermark_table.sql         # Metadata control table DDL
│   └── usp_write_watermark.sql            # Stored procedure to update high-watermark
├── databricks/
│   ├── 01_silver_transformation.ipynb     # Data cleansing, deduplication, and schema enforcement
│   ├── 02_gold_dim_branch.ipynb           # SCD Type 2 logic for Branch dimension
│   ├── 03_gold_dim_date.ipynb             # Date dimension generation
│   ├── 04_gold_dim_dealer.ipynb           # SCD Type 2 logic for Dealer dimension
│   ├── 05_gold_dim_model.ipynb            # SCD Type 2 logic for Car Model dimension
│   └── 06_gold_fact_sales.ipynb           # Surrogate key lookups & Fact table load
├── assets/
│   └── architecture_diagram.png
└── README.md

## Key Pipeline Workflows

### 1. Metadata-Driven Incremental Ingestion (ADF)
* **Initial Load (`source_prep`):** Loads baseline historical CSV records into Azure SQL DB and initializes the watermark control table.
* **Incremental Load (`increm_data_pipeline`):**
  1. `last_load` Lookup activity fetches the last successfully processed timestamp.
  2. `current_load` Lookup activity queries `MAX(Date_ID)` from the source table.
  3. `Copy_increm_data` extracts records between `last_load` and `current_load` into ADLS Gen2 (`bronze` container) in Parquet format.
  4. `WatermarkUpdate` executes `usp_write_watermark` to commit the new watermark value upon successful copy.

### 2. Medallion Transformations & Star Schema (Databricks)
* **Silver Layer:** Cleanses raw Parquet files from Bronze, standardizes data types, and writes ACID-compliant Delta tables to the `silver` container.
* **Gold Layer (Star Schema + SCD Type 2):**
  * Orchestrated via a Databricks Multi-Task Job (`Data-Model`) that executes dimension notebooks in parallel before loading `gold_fact_sales`.
  * Implements **SCD Type 2** using Delta Lake `MERGE` statements (`start_date`, `end_date`, `is_current`) to preserve historical changes across dealers, branches, and car models.

## How to Run / Setup
1. **Azure SQL Setup:** Run scripts in `/sql` to create the source tables, watermark table, and update stored procedure.
2. **ADLS Gen2 & Unity Catalog:** Provision storage account (`carsalessa`) with `bronze`, `silver`, and `gold` containers and configure external locations in Unity Catalog.
3. **ADF Deployment:** Import pipeline templates from `/adf` and configure Linked Services using Azure Key Vault.
4. **Databricks Workflow:** Import notebooks from `/databricks` and configure the multi-task job dependency graph (`Silver_data` → `Dim_*` → `Fact_table`).
