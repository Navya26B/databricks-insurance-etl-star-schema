Databricks Insurance ETL — Star Schema

Databricks-native ETL pipeline for insurance claims — Bronze/Silver/Gold
lakehouse with Auto Loader, Delta Live Tables, and a star schema for claims
fraud triage.

A Databricks-native lakehouse pipeline that ingests insurance claims and agent
data, applies incremental ingestion and data quality gating, and models the
result into a star schema built to surface claims and agents worth prioritising
for fraud investigation.

Business Problem

An insurance company's claims and fraud investigation team currently reviews
submitted claims without a systematic way to prioritise them, which doesn't
scale as claim volume grows. This project ingests claims and agent data,
cleans and validates it, and produces analytics-ready datasets that highlight
which claims show patterns consistent with fraud (early-claim timing,
abnormal reporting lag, amount outliers) and which agents show patterns
worth reviewing (claim concentration, tenure vs. flagged-claim rate).

This is a triage and prioritisation system, not a fraud verdict engine — it
surfaces claims and agents for human investigation rather than making
automated determinations.

Architecture

employee_data.csv / insurance_data.csv
            |
            v
   Unity Catalog Volume (landing zone)
            |
            v
   Auto Loader (incremental, schema-evolution-aware ingestion)
            |
            v
   Bronze (Delta tables, raw + ingestion metadata)
            |
   +--------+-----------------+
   v                          v
Silver: claims_data       Silver: employee_data
(Delta Live Tables,       (standalone notebook,
 data quality             cleaned + deduplicated)
 expectations,
 derived date fields)
            |                          |
            +------------+-------------+
                         v
                   Gold (star schema)
        dim_agent . dim_customer . dim_policy . dim_date
                   fact_claims
                         |
                         v
             Purpose-built marts (fraud signals,
             loss ratio, agent risk view)
                         |
                         v
               Databricks SQL / Lakeview Dashboard

Data Model

Star schema, grain of fact_claims = one row per claim transaction.


dim_agent — agent/employee attributes (SCD Type 1 — see
design decisions for why SCD2 was scoped out)
dim_customer — customer attributes (SCD Type 1)
dim_policy — policy attributes (SCD Type 1)
dim_date — standard calendar dimension, role-played across transaction,
loss, and report dates
fact_claims — claim-level measures and foreign keys to all dimensions


Data Quality

Data quality is enforced declaratively in the Silver layer using Delta Live
Tables expectations — invalid claim amounts, missing customer/agent/policy
references, and impossible date sequences are quarantined automatically.
Late-reported claims are flagged rather than dropped, since reporting delay is
itself a potential fraud signal, not a data error.

Tech Stack

Databricks (Auto Loader, Delta Live Tables, Unity Catalog, Delta Lake),
PySpark, SQL. See design decisions for the
reasoning behind each architectural choice.

Repository Structure

databricks-insurance-etl-star-schema/
├── README.md
├── docs/
│   ├── data_profile.md
│   ├── design_decisions.md
│   └── screenshots/
├── notebooks/
│   ├── 01_data_profiling.py
│   ├── 02_bronze_autoloader.py
│   ├── 03_silver_employee.py
│   └── silver_claims_pipeline/        # DLT pipeline source
├── .github/workflows/ci.yml
└── dataset/                            # see note below on inclusion

Project Status

In active development. Bronze and Silver layers are complete; Gold layer
(star schema + fraud signal marts) is in progress. See the
project roadmap for the full phased build plan.

How to Run


Create a Unity Catalog catalog and bronze/silver/gold schemas.
Upload employee_data.csv and insurance_data.csv to a Unity Catalog
Volume under the bronze schema.
Run notebooks/02_bronze_autoloader.py to populate Bronze tables.
Register notebooks/silver_claims_pipeline/ as a Delta Live Tables
pipeline, targeting the silver schema.
Run notebooks/03_silver_employee.py to populate the cleaned employee
table.
(Gold layer notebooks — to be added.)


Data Source

Dataset: Insurance Claims for Fraud Detection (Kaggle).
Not redistributed in this repository — download separately and place in
dataset/ before running, per the source's licensing terms.

Design Decisions

See docs/design_decisions.md for the reasoning
behind key architectural and data-modeling choices made throughout this
project, including trade-offs made due to Databricks Free Edition
limitations.
