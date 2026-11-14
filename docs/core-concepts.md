# Databricks and Spark: Core Concepts

[Back to the project README](../README.md)

## 1. Apache Spark

Apache Spark is a distributed compute engine that abstracts much of the complexity of parallel processing, allowing developers to focus on business logic.

### Driver, workers, and cluster

| Component | Responsibility |
| --- | --- |
| Driver | Runs the main application, builds execution plans, and schedules tasks on executors |
| Worker node | Provides compute resources and hosts executor processes |
| Executor | Runs tasks and stores application data in memory or on disk |
| Cluster | A group of machines that work together to run distributed workloads |
| Cluster manager | Allocates cluster resources to applications |

```text
Application / SQL query
          |
          v
Driver: builds plans and schedules tasks
          |
          +--------------------+
          v                    v
Worker node 1             Worker node 2
  Executor                  Executor
  Tasks on partitions       Tasks on partitions
          |                    |
          +--------------------+
                    |
                    v
            Output / results
```

The driver coordinates the application; the cluster manager allocates resources. Calling the driver the "master" can confuse these separate roles. An action, such as writing a table or calling `count()`, triggers execution of the planned work.

### Fault tolerance

Spark tracks the transformations used to produce data through lineage. When a partition is lost, Spark can recompute it from its dependencies and retry failed tasks. Executor failures can often be recovered this way, provided the source data remains available. Driver recovery depends on the deployment and application restart configuration.

Reference: [Spark cluster overview](https://spark.apache.org/docs/latest/cluster-overview.html) and [RDD fault tolerance](https://spark.apache.org/docs/latest/rdd-programming-guide.html).

## 2. SQL aliases (`AS`)

An alias gives a temporary name to a column or table within a query, making the query easier to read. It does not rename the stored column or table.

```sql
SELECT
    p.product_id AS id,
    p.sku AS product_sku
FROM ecommerce.gold.gld_dim_products AS p;
```

Here, `p` is a table alias, while `id` and `product_sku` are output column aliases.

### From SQL to results

```text
SQL query
    → Parsing and analysis
    → Catalyst logical optimization
    → Physical execution plan
    → Execution engine
    → Results
```

Catalyst optimizes the query plan. On Photon-enabled Databricks compute, Photon executes supported operations using a native vectorized engine. Unsupported operations can fall back to the Spark runtime. Photon is a Databricks capability; Apache Spark also executes queries without it.

Reference: [Databricks Photon](https://docs.databricks.com/aws/en/compute/photon).

## 3. Why use Parquet?

Parquet is an open, column-oriented file format designed for analytical workloads.

1. **Faster analytics:** Engines can read only the columns a query needs.
2. **Less storage:** Compression and encoding reduce file sizes.
3. **Big data support:** Spark, Hadoop, and other analytical engines support it.
4. **Schema support:** Files store column names and data types.
5. **Efficient ETL storage:** Processed datasets can be reused for downstream analysis.

For example, a query that selects only `product_id` and `quantity` can avoid reading unrelated column data. Actual performance depends on the query, file layout, and execution engine.

Parquet stores data files. Table features such as ACID transactions and version history come from a table format such as Delta Lake.

Reference: [Apache Parquet overview](https://parquet.apache.org/docs/overview/).

## 4. Coalesce: two different uses

### SQL `COALESCE`: choose the first non-null value

`COALESCE` returns the first non-null expression, or null if all its arguments are null.

```sql
SELECT COALESCE(NULL, 'Not Available') AS category_name;
-- Result: Not Available
```

In PySpark, the equivalent function is `pyspark.sql.functions.coalesce`:

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "category_name",
    F.coalesce(F.col("category_name"), F.lit("Not Available")),
)
```

An empty string is not null and is therefore not replaced by `COALESCE`.

### PySpark `DataFrame.coalesce(n)`: reduce partitions

`df.coalesce(n)` reduces the number of DataFrame partitions, normally without a shuffle. It does not increase the partition count.

```python
# Example only: df must already contain a loaded dataset.
fewer_partitions = df.coalesce(4)
```

Reducing partitions can reduce the number of output files, but too few partitions reduce parallelism. Use `repartition()` when increasing partitions or redistributing rows with a shuffle. Avoid routinely using `coalesce(1)` for large datasets.

References: [PySpark SQL coalesce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html) and [DataFrame.coalesce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html).

## 5. Medallion Architecture

Medallion Architecture organizes data into layers that progressively improve its quality and usefulness.

| Layer | Purpose | Examples in this project |
| --- | --- | --- |
| Bronze | Ingest source records and capture metadata | CSV ingestion, source file paths, ingestion timestamps |
| Silver | Clean, validate, and standardize records | Deduplication, numeric casts, date parsing, normalized values |
| Gold | Model data for business analysis | Product/customer/date dimensions and order-item facts |

```text
Raw sources → Bronze → Silver → Gold → BI / analytics
```

Reference: [Databricks Medallion Architecture](https://docs.databricks.com/aws/en/lakehouse/medallion).

## 6. Unity Catalog

Unity Catalog provides centralized governance for data and AI assets in Databricks, including organization, permissions, auditing, and lineage.

Tables use a three-level namespace:

```text
catalog.schema.table
ecommerce.gold.gld_fact_order_items
```

A volume provides governed access to files. The source notebooks read from the `raw` volume in the `ecommerce.source_data` schema:

```text
/Volumes/ecommerce/source_data/raw/
```

Reference: [Unity Catalog overview](https://docs.databricks.com/aws/en/data-governance/unity-catalog/).

## 7. Data lake and Delta Lake

### Data lake

A data lake is a centralized storage system that holds structured, semi-structured, and unstructured data, often in its original format. Examples include CSV files, JSON events, images, and Parquet datasets.

### Delta Lake

Delta Lake is an open-source storage framework that adds reliability to data lakes. A Delta table combines Parquet data files with a transaction log.

- **ACID transactions:** Atomicity, Consistency, Isolation, and Durability provide reliable table updates.
- **Schema enforcement:** Helps prevent incompatible writes.
- **Time travel:** Reads earlier table versions while the required log and data files remain available.
- **Reliable processing:** Supports batch and streaming workloads over Delta tables.

The project writes Delta tables using:

```python
df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce.bronze.brz_order_items"
)
```

Reference: [Delta Lake introduction](https://docs.delta.io/latest/delta-intro.html).

## 8. ETL, OLTP, and OLAP

| Concept | Full form | Purpose | Example |
| --- | --- | --- | --- |
| ETL | Extract, Transform, Load | Moves and processes data | Read source CSVs, clean fields, and write analytical tables |
| OLTP | Online Transaction Processing | Handles daily operational transactions | Place an order or update inventory |
| OLAP | Online Analytical Processing | Analyzes large volumes of data | Calculate monthly sales by product or region |

This project practices data transformation and table loading for analytical use. Its Gold dimensions and fact table provide a starting point for OLAP-style reporting.
