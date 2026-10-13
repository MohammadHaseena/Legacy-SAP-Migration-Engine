# Legacy SAP Migration Engine

## Project Overview

This project simulates the migration of a legacy SAP ETL pipeline to a modern cloud Lakehouse architecture using Databricks and PySpark.

The pipeline follows the Medallion Architecture (Bronze, Silver, Gold) to ingest, clean, transform, and aggregate SAP sales data for analytics.

---

## Architecture

```
SAP CSV Files
        │
        ▼
 Databricks Volume
        │
        ▼
 Bronze Layer
 (Raw Delta Table)
        │
        ▼
 Silver Layer
──────────────────────────────
✓ Remove Duplicates
✓ Remove Null Values
✓ Standardize Dates
✓ Quarantine Invalid Records
──────────────────────────────
        │
        ▼
 Broadcast Join
(Customer Master)
        │
        ▼
 Gold Layer
──────────────────────────────
Revenue by Region
Transaction Count
──────────────────────────────
        │
        ▼
 Business Analytics
```

---
## Sample Outputs
### Bronze Layer

![Bronze Layer](images/01_bronze_layer_output.png)

### Silver Layer

![Silver Layer](images/02_silver_layer_output.png)

### Gold Layer

![Gold Layer](images/03_gold_layer_output.png)

### Databricks Catalog

![Catalog](images/04_databricks_catalog.png)

### Quarantine Summary

![Quarantine Summary](images/05_quarantine_summary.png)

## Tech Stack

- Python
- PySpark
- Databricks
- Delta Lake
- Unity Catalog
- Faker
- Pandas

---

## Project Structure

```
Legacy-SAP-Migration-Engine
│
├── data_generator/
├── datasets/
├── databricks/
│   ├── 01_Bronze_Layer.py
│   ├── 02_Silver_Layer.py
│   └── 03_Gold_Layer.py
│
├── ingestion/
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Bronze Layer

- Read SAP CSV files
- Store raw data in Delta tables

---

## Silver Layer

- Remove duplicate records
- Remove NULL values
- Standardize mixed date formats
- Quarantine invalid records

---

## Gold Layer

- Broadcast Join with Customer Master
- Revenue aggregation by Region
- Transaction summary
- Delta optimization

---

## Test Data

- data_generator/generate_sap_data_small.py: small dataset (Faker + Pandas), for local testing
- data_generator/generate_sap_data_spark.py: 1M orders and 5K customers, run in Databricks,
  with injected defects: duplicates, negative prices, missing materials,
  mixed date formats, and orphan customers

---

## Features

- Medallion Architecture
- Delta Lake
- Data Quality Validation
- Broadcast Join
- Business Aggregations
- Production-style ETL Pipeline

---

## Future Improvements

- Azure Data Factory Integration
- Azure Blob Storage
- Automated Scheduling
- CI/CD Pipeline
- Power BI Dashboard
