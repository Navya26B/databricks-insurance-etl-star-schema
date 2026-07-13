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
