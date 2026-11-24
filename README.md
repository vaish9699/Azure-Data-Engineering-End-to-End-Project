# End-to-End Car Sales Data Engineering Pipeline (Azure & Databricks)

An enterprise-grade batch data engineering pipeline that ingests incremental car sales data from Azure SQL Database, processes it across a Medallion Architecture in Azure Data Lake Storage Gen2 (ADLS Gen2), and models a Star Schema with Slowly Changing Dimensions (SCD Type 2) in Azure Databricks.

## Architecture Overview

<img width="1408" height="768" alt="azure carsales project" src="https://github.com/user-attachments/assets/38e7ac4b-6bee-49ce-9bd2-04a1b407e82a" />

## Tech Stack
* **Cloud Platform:** Microsoft Azure
* **Source Database:** Azure SQL Database
* **Orchestration & Ingestion:** Azure Data Factory (ADF)
* **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Processing & Transformation:** Azure Databricks (PySpark, Spark SQL, Delta Lake)
* **Governance:** Unity Catalog
* **CI/CD & Version Control:** GitHub

## Repository Structure
<img width="793" height="411" alt="image" src="https://github.com/user-attachments/assets/6499dea7-b580-4e61-b0bc-c02de7b20f24" />


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
 
    <img width="1511" height="821" alt="Pasted 24-11-2026 at 11 06 45" src="https://github.com/user-attachments/assets/0ee22149-8871-4a73-8cae-bab5a4e97404" />


## How to Run / Setup
1. **Azure SQL Setup:** Run scripts in `/sql` to create the source tables, watermark table, and update stored procedure.
2. **ADLS Gen2 & Unity Catalog:** Provision storage account (`carsalessa`) with `bronze`, `silver`, and `gold` containers and configure external locations in Unity Catalog.
3. **ADF Deployment:** Import pipeline templates from `/adf` and configure Linked Services using Azure Key Vault.
4. **Databricks Workflow:** Import notebooks from `/databricks` and configure the multi-task job dependency graph (`Silver_data` → `Dim_*` → `Fact_table`).
