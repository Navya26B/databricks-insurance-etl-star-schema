import dlt
from pyspark.sql import functions as f

catalog = "claims_fraud_lakehouse"

@dlt.table(
    name = "dim_date",
    comment = "Standard calender dimension, froom 2015 to 2027 full dates."
)

def dim_date():
    return (
        spark.sql("""
                  select explode(sequence(to_date('2015-01-01'), to_date('2027-12-31'), interval 1 day)) AS full_date""")
        .withColumn("date_sk", f.date_format("full_Date", "yyyyMMdd").cast("int"))
        .withColumn("year", f.year("full_date"))
        .withColumn("month", f.month("full_date"))
        .withColumn("day", f.dayofmonth("full_date"))
        .withColumn("day_of_week", f.dayofweek("full_date"))
        .withColumn("quarter", f.quarter("full_date"))
        .withColumn("is_weekend", f.dayofweek("full_date").isin(1,7))
    )


@dlt.table(
    name = "dim_agent",
    comment = "Agent basically Employee dimension, SCD Type 1 becuase free edision doesnt support SCD Type 2."
)

def dim_agent():
    return(
        spark.table(f"{catalog}.silver.silver_employee")
        .select(
            f.col("AGENT_ID").alias("agent_id"),
            f.col("AGENT_NAME").alias("agent_name"),
            f.col("DATE_OF_JOINING").alias("date_of_joining"),
            f.col("ADDRESS_LINE1").alias("address_line1"),
            f.col("ADDRESS_LINE2").alias("address_line2"),
            f.col("CITY").alias("city"),
            f.col("STATE").alias("state"),
            f.col("POSTAL_CODE").alias("postal_code"),
            f.col("EMP_ROUTING_NUMBER").alias("emp_routing_number"),
            f.col("EMP_ACCT_NUMBER").alias("account_number"),
        )
        .withColumn("Agent_sk", f.monotonically_increasing_id())
    )



@dlt.table(
    name = "dim_customer",
    comment = "Customer dimension, decomposed from claims."
)


def dim_customer():
    return(
        spark.table(f"{catalog}.silver.silver_claims")
        .select(
            f.col("CUSTOMER_ID").alias("customer_id"),
            f.col("CUSTOMER_NAME").alias("customer_name"),
            f.col("SSN").alias("ssn"),
            f.col("MARITAL_STATUS").alias("marital_status"),
            f.col("AGE").alias("age"),
            f.col("EMPLOYMENT_STATUS").alias("employment_status"),
            f.col("NO_OF_FAMILY_MEMBERS").alias("no_of_family_members"),
            f.col("RISK_SEGMENTATION").alias("risk_segmentation"),
            f.col("HOUSE_TYPE").alias("house_type"),
            f.col("SOCIAL_CLASS").alias("social_class"),
            f.col("CUSTOMER_EDUCATION_LEVEL").alias("customer_education_level"),
            f.col("ROUTING_NUMBER").alias("routing_number"),
            f.col("ADDRESS_LINE1").alias("address_line1"),
            f.col("ADDRESS_LINE2").alias("address_line2"),
            f.col("CITY").alias("city"),
            f.col("STATE").alias("state"),
            f.col("POSTAL_CODE").alias("postal_code"),
        )
        .dropDuplicates(["customer_id"])
        .withColumn("Customer_sk", f.monotonically_increasing_id()
    )

)



@dlt.table(
    name = "dim_policy",
    comment = "Policy dimension, decomposed from claims."
)

def dim_policy():
    return(
        spark.table(f"{catalog}.silver.silver_claims")
        .select(
            f.col("POLICY_NUMBER").alias("policy_number"),
            f.col("POLICY_EFF_DT").alias("policy_eff_dt"),
            f.col("INSURANCE_TYPE").alias("insurance_type")
        )
        .dropDuplicates(["policy_number"])
        .withColumn("Policy_sk", f.monotonically_increasing_id())
    )




@dlt.table(
    name="fact_claim",
    comment="Fact table, one row per claim transaction"
)
def fact_claim():
    claims = spark.table(f"{catalog}.silver.silver_claims")
    dim_agent = spark.table(f"{catalog}.gold.dim_agent").select("agent_id", "agent_sk")
    dim_customer = spark.table(f"{catalog}.gold.dim_customer").select("customer_id", "customer_sk")
    dim_policy = spark.table(f"{catalog}.gold.dim_policy").select("policy_number", "policy_sk")
    dim_date_txn = spark.table(f"{catalog}.gold.dim_date").select(
        f.col("date_sk").alias("txn_date_sk"), f.col("full_date").alias("txn_full_date"))
    dim_date_loss = spark.table(f"{catalog}.gold.dim_date").select(
        f.col("date_sk").alias("loss_date_sk"), f.col("full_date").alias("loss_full_date"))
    dim_date_report = spark.table(f"{catalog}.gold.dim_date").select(
        f.col("date_sk").alias("report_date_sk"), f.col("full_date").alias("report_full_date"))

    return (
        claims
        .join(dim_agent, claims.AGENT_ID == dim_agent.agent_id, "left")
        .join(dim_customer, claims.CUSTOMER_ID == dim_customer.customer_id, "left")
        .join(dim_policy, claims.POLICY_NUMBER == dim_policy.policy_number, "left")
        .join(dim_date_txn, f.to_date(claims.TXN_DATE_TIME) == dim_date_txn.txn_full_date, "left")
        .join(dim_date_loss, claims.LOSS_DT == dim_date_loss.loss_full_date, "left")
        .join(dim_date_report, claims.REPORT_DT == dim_date_report.report_full_date, "left")
        .select(
            f.col("TRANSACTION_ID").alias("transaction_id"),
            "agent_sk", "customer_sk", "policy_sk",
            "txn_date_sk", "loss_date_sk", "report_date_sk",
            f.col("CLAIM_AMOUNT").alias("claim_amount"),
            f.col("PREMIUM_AMOUNT").alias("premium_amount"),
            f.col("CLAIM_STATUS").alias("claim_status"),
            f.col("claim_status_desc"),
            f.col("INCIDENT_SEVERITY").alias("incident_severity"),
            f.col("AUTHORITY_CONTACTED").alias("authority_contacted"),
            f.col("ANY_INJURY").alias("any_injury"),
            f.col("POLICE_REPORT_AVAILABLE").alias("police_report_available"),
            f.col("INCIDENT_STATE").alias("incident_state"),
            f.col("INCIDENT_CITY").alias("incident_city"),
            f.col("INCIDENT_HOUR_OF_THE_DAY").alias("incident_hour_of_day"),
            f.col("reporting_lag_days"),
            f.col("policy_to_loss_days"),
        )
    )