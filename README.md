# Databricks Learning

Hands-on notes and Databricks notebooks for learning Apache Spark, SQL, Delta Lake, Unity Catalog, and the Medallion Architecture through an ecommerce data engineering project.

Repository: [Hitesh-s0lanki/databricks-learning](https://github.com/Hitesh-s0lanki/databricks-learning)

## Project overview

The project ingests ecommerce CSV files, cleans and standardizes them, and builds dimension and fact tables for analytics.

```text
Source CSV files in a Unity Catalog volume
                    |
                    v
Bronze: raw ingestion and source metadata
                    |
                    v
Silver: deduplication, cleaning, and type conversion
                    |
                    v
Gold: product, customer, and date dimensions + order-item facts
```

The notebooks use the `ecommerce` catalog and the `bronze`, `silver`, and `gold` schemas. Tables are stored in Delta format.

## Repository structure

```text
databricks-learning/
├── README.md
├── .gitignore
├── docs/
│   └── core-concepts.md
├── setup/
│   └── setup_catalog.py
├── medallion_processing_dim/
│   ├── dim_bronze.py
│   ├── dim_silver.py
│   └── dim_gold.py
└── medallion_processing_fact/
    ├── fact_bronze.py
    ├── fact_silver.py
    └── fact_gold.py
```

The `.py` files are Databricks notebook source exports with `# COMMAND ----------` cell separators and SQL magic cells.

## Getting started

### 1. Open the project

Clone the repository locally:

```bash
git clone https://github.com/Hitesh-s0lanki/databricks-learning.git
cd databricks-learning
```

Add the repository as a Git folder in Databricks, or import the notebook source files into your workspace.

### 2. Prepare Databricks

Use a workspace with Unity Catalog and compute that supports Python notebooks, Delta tables, and Unity Catalog volumes. You need permission to create or use the `ecommerce` catalog, create schemas and tables, and read and write the source volume.

Run [setup/setup_catalog.py](setup/setup_catalog.py) to create the catalog and the three processing schemas. If catalog creation is restricted, ask your workspace administrator to provision it first.

Create the source schema and a managed volume in a Databricks SQL cell:

```sql
CREATE SCHEMA IF NOT EXISTS ecommerce.source_data;
CREATE VOLUME IF NOT EXISTS ecommerce.source_data.raw;
```

### 3. Upload the source files

Prepare CSV files with headers matching the schemas defined in the Bronze notebooks, and upload them to the following directories:

```text
/Volumes/ecommerce/source_data/raw/
├── brands/*.csv
├── category/*.csv
├── products/*.csv
├── customers/*.csv
├── date/*.csv
└── order_items/landing/*.csv
```

Source datasets are not included in this repository. Use the `StructType` definitions in `dim_bronze.py` and `fact_bronze.py` as the field reference. The product notebook populates `file_name` and `ingest_timestamp` from ingestion metadata.

### 4. Run the notebooks

Run each notebook from top to bottom in this order:

| Step | Notebook | Purpose |
| --- | --- | --- |
| 1 | `setup/setup_catalog.py` | Create the catalog and processing schemas |
| 2 | `medallion_processing_dim/dim_bronze.py` | Ingest brands, categories, products, customers, and calendar data |
| 3 | `medallion_processing_fact/fact_bronze.py` | Ingest order items |
| 4 | `medallion_processing_dim/dim_silver.py` | Clean and standardize dimension data |
| 5 | `medallion_processing_fact/fact_silver.py` | Clean and standardize order items |
| 6 | `medallion_processing_dim/dim_gold.py` | Build product, customer, and date dimensions |
| 7 | `medallion_processing_fact/fact_gold.py` | Build the order-item fact table and sales measures |

These learning notebooks overwrite their destination tables when rerun. Use a dedicated learning catalog. The Gold fact notebook uses fixed example currency rates dated 2025-10-15; replace them with an appropriate rate source for real reporting.

### 5. Inspect the results

Run these queries in Databricks after completing the pipeline:

```sql
SHOW TABLES IN ecommerce.gold;

SELECT * FROM ecommerce.gold.gld_dim_products LIMIT 10;
SELECT * FROM ecommerce.gold.gld_dim_customers LIMIT 10;
SELECT * FROM ecommerce.gold.gld_dim_date LIMIT 10;
SELECT * FROM ecommerce.gold.gld_fact_order_items LIMIT 10;
```

The pipeline requires Databricks and the source data for end-to-end validation. It is not configured as a standalone local Python application.

## Core concepts

Read the [core concepts guide](docs/core-concepts.md) for explanations and examples covering:

- Apache Spark: driver, workers, executors, clusters, and fault tolerance.
- SQL aliases, Catalyst optimization, and Photon execution.
- Parquet and its benefits for analytics.
- SQL `COALESCE` and PySpark `DataFrame.coalesce()`.
- Bronze, Silver, and Gold layers in the Medallion Architecture.
- Unity Catalog, Delta Lake, and data lakes.
- ETL, OLTP, and OLAP.

## Learning goals

- Understand how Spark distributes work across a cluster.
- Read CSV data with explicit schemas and capture ingestion metadata.
- Clean data with PySpark transformations and SQL.
- Build Delta tables through Bronze, Silver, and Gold layers.
- Organize governed data using Unity Catalog.
- Build dimension and fact tables for analytical queries.
