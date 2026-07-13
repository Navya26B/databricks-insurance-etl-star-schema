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

---

# Architecture

```text
employee_data.csv / insurance_data.csv
                │
                ▼
     Unity Catalog Volume (Landing Zone)
                │
                ▼
 Auto Loader (Incremental Ingestion)
                │
                ▼
      Bronze Delta Tables
                │
     ┌──────────┴──────────┐
     ▼                     ▼
Silver Claims        Silver Employees
 (Delta Live Tables)   (PySpark)
     │                     │
     └──────────┬──────────┘
                ▼
        Gold Star Schema
                │
   ┌────────────┼────────────┐
   ▼            ▼            ▼
fact_claims  Dimension Tables  Business Marts
                │
                ▼
 Databricks SQL / Lakeview Dashboard
```

---

# Tech Stack

| Category | Technologies |
|-----------|--------------|
| Platform | Databricks |
| Storage | Delta Lake |
| Governance | Unity Catalog |
| Ingestion | Auto Loader |
| Processing | PySpark, Delta Live Tables (DLT) |
| Querying | Spark SQL |
| Data Model | Star Schema |
| Architecture | Medallion (Bronze, Silver, Gold) |

---

# Data Model

The Gold layer follows a dimensional star schema.

### Fact Table

- **fact_claims**
  - One row per insurance claim transaction

### Dimension Tables

- **dim_agent**
- **dim_customer**
- **dim_policy**
- **dim_date**

Current implementation uses **Slowly Changing Dimension (SCD) Type 1**.

---

# Data Quality

Data quality is enforced declaratively in the Silver layer using **Delta Live Tables Expectations**.

Validation rules include:

- Invalid claim amounts
- Missing customer references
- Missing agent references
- Missing policy references
- Invalid date sequences

Late-reported claims are **flagged rather than removed**, since reporting delay is itself a useful fraud signal.

---

# Repository Structure

```text
databricks-insurance-etl-star-schema/
├── README.md
├── dataset/
├── docs/
│   ├── data_profile.md
│   ├── design_decisions.md
│   └── screenshots/
├── notebooks/
│   ├── 01_data_profiling.py
│   ├── 02_bronze_autoloader.py
│   ├── 03_silver_employee.py
│   └── silver_claims_pipeline/
└── .github/
    └── workflows/
```

---

# Pipeline Layers

## Bronze

- Incremental ingestion using Auto Loader
- Raw Delta tables
- Schema evolution support
- Ingestion metadata

## Silver

### Claims Pipeline

- Delta Live Tables (DLT)
- Data quality expectations
- Data cleansing
- Derived business fields
- Validation rules

### Employee Pipeline

- Cleaning
- Deduplication
- Standardisation

## Gold

### Star Schema

- fact_claims
- dim_agent
- dim_customer
- dim_policy
- dim_date

### Business Marts

- Fraud signals
- Agent risk metrics
- Loss ratio reporting

---

# How to Run

### 1. Create Unity Catalog Objects

Create:

- Catalog
- Bronze schema
- Silver schema
- Gold schema

### 2. Upload Source Files

Upload the following files to a Unity Catalog Volume:

- `employee_data.csv`
- `insurance_data.csv`

### 3. Run Bronze Pipeline

```python
notebooks/02_bronze_autoloader.py
```

### 4. Deploy Silver Claims Pipeline

Register the following directory as a Delta Live Tables pipeline:

```text
notebooks/silver_claims_pipeline/
```

### 5. Populate Employee Table

```python
notebooks/03_silver_employee.py
```

### 6. Build Gold Layer

Run the Gold notebooks (coming soon).

---

# Dataset

**Source:** Insurance Claims for Fraud Detection (Kaggle)

The dataset is **not redistributed** in this repository.

Download it separately and place it inside:

```text
dataset/
```

before running the project.

---

# Project Status

| Component | Status |
|-----------|--------|
| Bronze Layer | ✅ Complete |
| Silver Layer | ✅ Complete |
| Gold Star Schema | 🚧 In Progress |
| Business Marts | 🚧 In Progress |
| Dashboard | 🚧 Planned |
| CI/CD | 🚧 Planned |

---

# Documentation

Additional documentation is available in the **docs/** folder.

- **data_profile.md** – Dataset profiling and exploration
- **design_decisions.md** – Architecture choices and trade-offs
- **screenshots/** – Pipeline and dashboard screenshots
