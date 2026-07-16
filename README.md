# Databricks Insurance ETL – Lakehouse & Star Schema

> **A Databricks pipeline that ingests insurance claims data, cleans and validates it, and organizes it into an analytics-ready star schema — built to help fraud investigators prioritise which claims and agents need a closer look.**

---

## What This Project Does

Raw insurance claims and agent data are loaded automatically, checked for
quality issues, and reshaped into a clean, well-organized set of tables
that are easy to query and report on.

> **Note:** This is a **triage tool**, not a fraud detector. It helps
> surface claims and agents worth investigating — it doesn't decide who's
> guilty of anything.

---

## The Problem This Solves

Insurance companies get thousands of claims. Reviewing every single one by
hand doesn't scale. This project helps investigators focus on the claims
most worth their time by highlighting things like:

- Claims filed suspiciously soon after a policy started
- Claims reported a long time after the incident happened
- Unusually high claim amounts
- Agents handling an unusually large share of claims
- Agents with a high rate of flagged claims relative to how long they've worked there

The goal is to help a human decide faster — not to make the decision for them.

## Architecture

<p align="center">
  <img src="Assets/Databricks Insurance Architecture.png" alt="Databricks Insurance ETL Architecture" width="900"/>
</p>

The whole pipeline runs automatically as one Databricks Job — see
[How It Runs](#how-it-runs) below.

## How the Data Is Organized

The final layer of data (Gold) is organized as a **star schema** — one
central table of facts, surrounded by supporting tables of details.

**The main table:**
- **fact_claims** — one row per claim, with the key numbers (claim amount,
  premium, how late it was reported) and links to everything else below

**The supporting tables:**
- **dim_agent** — details about each agent
- **dim_customer** — details about each customer
- **dim_policy** — details about each policy
- **dim_date** — a calendar table, reused for the claim date, loss date,
  and report date

> **Note:** Agent history tracking (keeping a record of past bank/address
> changes) was originally planned but left out — see
> [What's Not Included](#whats-not-included) for why.

---

## Data Quality

Before data reaches the clean layer, it's checked automatically:

- Claims with an invalid amount, or missing a customer, agent, or policy,
  are set aside rather than let through
- Claims where the policy started *after* the loss happened are removed —
  that's not logically possible
- Claims reported unusually late are **kept and flagged**, not removed —
  a late report can itself be a sign something's worth investigating

---

## How It Runs

The whole pipeline runs as one **Databricks Job** — nothing is done by
hand:

```
Load employee data  ---+
                         +--> Clean & validate --> Build the star schema
Load claims data     ---+
```

- The two data loads happen **at the same time**, since they're
  independent of each other
- Cleaning only starts once both loads finish successfully
- If anything fails, it retries automatically and sends an alert

---

## Automated Checks (CI/CD)

Every code change goes through an automatic check before it's allowed into
the main branch — it verifies the code is properly formatted and free of
obvious issues.

---

## Built With

Databricks (Auto Loader, Delta Live Tables, Unity Catalog, Delta Lake,
Databricks Jobs), PySpark, SQL, GitHub Actions.

---

## Project Layout

```
databricks-insurance-etl-star-schema/
├── README.md
├── Assets/                              # diagrams and screenshots
├── notebooks/
│   ├── data_profiling.py                # initial data exploration
│   ├── Bronze_Autoload.py               # loads raw data in
│   ├── my_silver_transformation.py      # cleans and validates the data
│   └── my_gold_transformation.py        # builds the star schema
├── docs/
│   ├── data_profile.md
│   └── design_decisions.md
└── .github/workflows/ci.yml
```

---

## Where Things Stand

**Done:** loading the data automatically, cleaning and validating it,
building the star schema, running the whole thing as one automated job,
and checking every code change before it merges.

**Not done yet:** the actual fraud-flagging logic (built on top of the
star schema), a dashboard to view the results, and ongoing monitoring.

---

## What's Not Included

- **Agent history tracking wasn't built.** The tool that would normally
  handle this (Delta Live Tables' change-tracking feature) needs a paid
  Databricks tier that isn't available on the free version used for this
  project. A manual version was built and tested, but wasn't kept — it felt
  like working around the platform rather than using it properly. Full
  reasoning is in [design decisions](docs/design_decisions.md).
- **One column was deliberately left out of the fraud logic.** The
  "incident hour" field was checked during data exploration and found to
  be spread almost perfectly evenly across all 24 hours — real accident
  data doesn't look like that, which suggests this part of the dataset is
  artificially generated. Using it as a signal would mean detecting noise,
  not a real pattern.
- Ran into (and worked around) a known bug specific to Databricks' free
  tier while setting up the final layer — confirmed via Databricks'
  community forums as a platform issue, not something wrong with this
  pipeline.

---

## Running This Yourself

1. Set up a Databricks catalog with four schemas: `raw`, `bronze`,
   `silver`, `gold`, plus a storage volume under `raw`.
2. Upload `employee_data.csv` and `insurance_data.csv` into that volume,
   each in its own folder.
3. Set up `my_silver_transformation.py` and `my_gold_transformation.py` as
   Delta Live Tables pipelines.
4. Create a Databricks Job matching the flow shown under
   [How It Runs](#how-it-runs).
5. Run it.

## Data Source

Dataset: *Insurance Claims for Fraud Detection* (Kaggle). Not included in
this repository — download it separately and follow the source's licensing
terms.

## More Detail

See [docs/design_decisions.md](docs/design_decisions.md) for the reasoning
behind the bigger decisions made throughout this project.
