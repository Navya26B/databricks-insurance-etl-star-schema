# Databricks Insurance ETL – Lakehouse & Star Schema

> **A production-style Databricks Lakehouse ETL pipeline for insurance claims using Auto Loader, Delta Live Tables (DLT), Unity Catalog, Delta Lake, and a Medallion (Bronze–Silver–Gold) architecture. The pipeline delivers an analytics-ready star schema to support claims fraud triage and business reporting.**

---

## Overview

This project demonstrates how to build an end-to-end ETL pipeline on the Databricks Lakehouse Platform.
Raw insurance claims and employee data are incrementally ingested, validated, transformed through Bronze, Silver, and Gold layers, and modeled into a dimensional star schema for analytics.

> **Note:** This project is a **fraud triage system**, not a fraud prediction model. It identifies claims and agents that should be prioritised for human investigation rather than making automated fraud decisions.

---

# Business Problem

Insurance companies process thousands of claims every day, making manual review difficult and expensive.
This project helps investigators prioritise claims by surfacing records that exhibit patterns commonly associated with fraud, including:

- Early claims after policy inception
- Long reporting delays
- High claim amounts
- Agents with unusually high claim concentrations
- High flagged-claim rates relative to agent tenure

The output is intended to support **human investigation**, not automatically classify claims as fraudulent.

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

> **Note:** SCD Type 2 was originally planned for `dim_agent`, to track
> agent bank/address changes over time. It was scoped out — see
> [Known Limitations](#known-limitations).

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

# CI/CD

Every pull request runs an automated GitHub Actions check that lints and
verifies formatting (`black`, `ruff`) across all notebook source code before
it's allowed to merge into `main`.

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
├── notebooks/
│   ├── data_profiling.py
│   ├── Bronze_Autoload.py               # parameterized, run twice as parallel Job tasks
│   ├── my_silver_transformation.py      # DLT pipeline: silver_claims + silver_employee
│   └── my_gold_transformation.py        # DLT pipeline: star schema
├── docs/
│   ├── data_profile.md
│   └── design_decisions.md
└── .github/workflows/ci.yml
```

---

# Project Status

**Built and running:** Bronze ingestion (Auto Loader, incremental), Silver
(Delta Live Tables, data quality expectations), Gold (star schema — four
dimensions, one fact table), full pipeline orchestration via Databricks Job,
CI linting on every pull request.

**Not yet built:** the fraud-signal marts (claim-level and agent-level risk
flags built on top of the star schema), the downstream dashboard, and
platform monitoring. These sit directly on top of the foundation above.

---

# Known Limitations

- **SCD Type 2 on the agent dimension was scoped out.** `apply_changes()`
  (Delta Live Tables' native CDC/history-tracking feature) requires a Pro or
  Advanced DLT product edition, unavailable on Databricks Free Edition. A
  manual merge-based SCD2 implementation was prototyped and working, but
  deliberately not used — it read as a workaround rather than idiomatic
  Databricks design. `dim_agent` is currently SCD Type 1. Full reasoning in
  [design decisions](docs/design_decisions.md).
- **`INCIDENT_HOUR_OF_THE_DAY` was profiled and excluded as a fraud signal.**
  The column is near-uniformly distributed across all 24 hours, inconsistent
  with real-world incident timing and indicative of synthetic data
  generation. Including it as a signal would fit noise, not a real pattern.
- Encountered and worked around a known Databricks Free Edition Unity
  Catalog platform issue during Gold pipeline setup (`PERMISSION_DENIED:
  Can not move tables across arclight catalogs`), confirmed via the
  Databricks Community forums as a platform-level issue, not a pipeline
  defect.

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

# Design Decisions

See [docs/design_decisions.md](docs/design_decisions.md) for the reasoning
behind every non-obvious architectural and data-modeling choice made
throughout this project, including trade-offs made due to Databricks Free
Edition limitations.
