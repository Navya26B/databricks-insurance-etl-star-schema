
import dlt
from pyspark.sql import functions as f
from pyspark.sql.types import *
catalog = "claims_fraud_lakehouse"

# Helper function - defined at the top level
def to_snake_case(df):
    for col in df.columns:
        new_name = col.lower().replace(" ", "_")
        df = df.withColumnRenamed(col, new_name)
    return df

# ----------------------------
# Silver Employee Table
# ----------------------------
@dlt.table(
    name="silver_employee",
    comment="Cleaned employee data"
)
def silver_employee():

    return (
        spark.readStream.table(f"{catalog}.bronze.employee_data")
        .dropDuplicates()
    )


# ----------------------------
# Silver Claims Table
# ----------------------------
@dlt.table(
    name="silver_claims",
    comment="Cleansed claims transactions with data quality checks and derived columns."
)
def silver_claims():

    df = spark.readStream.table(f"{catalog}.bronze.insurance_data")

    return (
        df
        .withColumn(
            "reporting_lag_days",
            f.datediff(f.col("REPORT_DT"), f.col("LOSS_DT"))
        )
        .withColumn(
            "policy_to_loss_days",
            f.datediff(f.col("LOSS_DT"), f.col("POLICY_EFF_DT"))
        )
        .withColumn(
            "claim_status_desc",
            f.when(f.col("CLAIM_STATUS") == "A", "Approved")
             .when(f.col("CLAIM_STATUS") == "D", "Denied")
             .when(f.col("CLAIM_STATUS") == "P", "Pending")
             .otherwise("Unknown")
        )
        .dropDuplicates(["TRANSACTION_ID"])

        .withColumn("TXN_DATE_TIME", f.to_timestamp(f.col("TXN_DATE_TIME")))
        .withColumn("POLICY_EFF_DT", f.to_date(f.col("POLICY_EFF_DT")))
        .withColumn("LOSS_DT", f.to_date(f.col("LOSS_DT")))
        .withColumn("REPORT_DT", f.to_date(f.col("REPORT_DT")))
        .withColumn("PREMIUM_AMOUNT", f.col("PREMIUM_AMOUNT").cast(DecimalType(18, 2)))
        .withColumn("CLAIM_AMOUNT", f.col("CLAIM_AMOUNT").cast(DecimalType(18, 2)))
        .withColumn("POSTAL_CODE", f.col("POSTAL_CODE").cast(IntegerType()))
        .withColumn("AGE", f.col("AGE").cast(IntegerType()))
        .withColumn("TENURE", f.col("TENURE").cast(IntegerType()))
        .withColumn("NO_OF_FAMILY_MEMBERS", f.col("NO_OF_FAMILY_MEMBERS").cast(IntegerType()))
        .withColumn("ROUTING_NUMBER", f.col("ROUTING_NUMBER").cast(LongType()))
        .withColumn("ANY_INJURY", f.col("ANY_INJURY").cast(BooleanType()))
        .withColumn("POLICE_REPORT_AVAILABLE", f.col("POLICE_REPORT_AVAILABLE").cast(BooleanType()))
        .withColumn("INCIDENT_HOUR_OF_THE_DAY", f.col("INCIDENT_HOUR_OF_THE_DAY").cast(IntegerType()))

    )




