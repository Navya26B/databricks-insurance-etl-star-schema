# Databricks Insurance ETL – Lakehouse & Star Schema

> **A Databricks Lakehouse ETL pipeline for insurance claims using Auto Loader, Delta Live Tables (DLT), Unity Catalog, Delta Lake, and a Medallion (Bronze–Silver–Gold) architecture. The pipeline delivers a governed, analytics-ready star schema for insurance claims reporting and analysis.**

---

## Overview

This project demonstrates how to build an end-to-end ETL pipeline on the
Databricks Lakehouse Platform. Raw insurance claims and agent data are
incrementally ingested, validated, and transformed through Bronze, Silver,
and Gold layers, then modeled into a dimensional star schema suitable for
business reporting and analysis.


---

# Business Problem

Insurance claims data typically arrives as flat, denormalized files —
customer, agent, and policy details repeated across every claim row, with
no data quality checks and no structure suited to reporting. This project
turns that raw data into a governed, dimensional model that supports
reliable business reporting: claims by policy type, loss ratios, agent
claim volumes, and similar standard insurance analytics — built on
incrementally-updating, quality-checked data rather than a one-off export.

# Architecture

<p align="center">
  <img src="Assets/Databricks Insurance Architecture.png" alt="Databricks Insurance ETL Architecture" width="900"/>
</p>

The pipeline runs end to end as a single orchestrated Databricks Job — see
[Orchestration](#orchestration) below.

# Data Model

The Gold layer follows a dimensional star schema.

### Fact Table

- **fact_claims**
  - One row per insurance claim transaction
  - Foreign keys to every dimension below, plus claim-level measures
    (claim amount, premium amount, reporting lag, and other derived fields)

### Dimension Tables

- **dim_agent** — agent/employee attributes (SCD Type 1)
- **dim_customer** — customer attributes, decomposed from claims (SCD Type 1)
- **dim_policy** — policy attributes, decomposed from claims (SCD Type 1)
- **dim_date** — standard calendar dimension, role-played across
  transaction, loss, and report dates

---

# Data Quality

Data quality is enforced declaratively in the Silver layer using Delta Live
Tables expectations:

- Invalid claim amounts, and missing customer, agent, or policy references
  are quarantined automatically
- Claims where the policy start date falls after the loss date are dropped
  as logically invalid
- Claims reported unusually late are **flagged, not dropped** — reporting
  delay is itself a potential fraud signal, not a data error, so it's
  preserved rather than discarded

---

# Orchestration

The full pipeline runs as a single **Databricks Job**, not manual steps:

```
bronze_ingest_employee  ---+
                            +--> silver (DLT) --> gold_star_schema (DLT)
bronze_ingest_claims     ---+
```

- The two Bronze ingestion tasks run in **parallel**, since they're
  independent data sources
- Silver only starts once both Bronze tasks succeed
- Each task has a configured retry policy
- The Job sends a failure notification if any task fails

---

# Tech Stack

Databricks (Auto Loader, Delta Live Tables, Unity Catalog, Delta Lake,
Databricks Jobs), PySpark, SQL, GitHub Actions.

---

# Repository Structure

```
databricks-insurance-etl-star-schema/
├── README.md
├── Assets/                              # architecture diagram, screenshots
├── Datasets/
│   ├── employee_data.csv
│   ├── insurance_data.csv
├── notebooks/
│   ├── data_profiling.py
│   ├── Bronze_Autoload.py               # parameterized, run twice as parallel Job tasks
│   ├── my_silver_transformation.py      # DLT pipeline: silver_claims + silver_employee
│   └── my_gold_transformation.py        # DLT pipeline: star schema
├── docs/
│   ├── data_profile.md
└── .github/workflows/ci.yml
```

---

# Project Status

**Built and running:** Bronze ingestion (Auto Loader, incremental), Silver
(Delta Live Tables, data quality expectations), Gold (star schema — four
dimensions, one fact table), full pipeline orchestration via Databricks Job,
CI linting on every pull request.


---

# How to Run

1. Create a Unity Catalog catalog and `raw` / `bronze` / `silver` / `gold`
   schemas, plus a landing Volume under `raw`.
2. Upload `employee_data.csv` and `insurance_data.csv` to the landing
   Volume, each in their own subfolder.
3. Register `my_silver_transformation.py` and `my_gold_transformation.py`
   as Delta Live Tables pipelines, targeting the `silver` and `gold`
   schemas respectively.
4. Create a Databricks Job with the task graph described under
   [Orchestration](#orchestration), using `Bronze_Autoload.py` for the two
   Bronze tasks (run once per source table, with matching widget
   parameters) and the two DLT pipelines for Silver and Gold.
5. Run the Job.

---

# Data Source

Dataset: Insurance Claims for Fraud Detection (Kaggle). Not redistributed
in this repository — download separately and place in a Unity Catalog
Volume before running, per the source's licensing terms.

---

