# Databricks notebook source
employee_path = "/Volumes/claims_fraud_lakehouse/raw/raw_data/employee_data/employee_data.csv"
claims_path   = "/Volumes/claims_fraud_lakehouse/raw/raw_data/insurance_data/insurance_data.csv"
 
df_employee = spark.read.option("header", True).option("inferSchema", True).csv(employee_path)
df_claims   = spark.read.option("header", True).option("inferSchema", True).csv(claims_path)

# COMMAND ----------

df_employee.printSchema()
df_claims.printSchema()

# COMMAND ----------

df_claims.count()

# COMMAND ----------

df_employee.count()

# COMMAND ----------

df_employee.count()

from pyspark.sql.functions import col, sum as spark_sum, when

# Count nulls for each column in df_employee
null_counts = [
    spark_sum(when(col(column).isNull(), 1).otherwise(0)).alias(column)
    for column in df_employee.columns
]
display(df_employee.select(null_counts))

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when

# Count nulls for each column in df_employee
null_counts = [
    spark_sum(when(col(column).isNull(), 1).otherwise(0)).alias(column)
    for column in df_claims.columns
]
display(df_claims.select(null_counts))

# COMMAND ----------

df_claims.select("INSURANCE_TYPE").distinct().show(truncate=False)

# COMMAND ----------


# Claim status distribution
df_claims.groupBy("CLAIM_STATUS").count().show()


# COMMAND ----------


# Incident severity distribution  
df_claims.groupBy("INCIDENT_SEVERITY").count().show()


# COMMAND ----------


# Confirm the join actually works — count claims with no matching agent
orphaned_claims = df_claims.join(df_employee, "AGENT_ID", "left_anti").count()
print(f"Claims with no matching AGENT_ID in employee data: {orphaned_claims}")


# COMMAND ----------


# Odd-hour incident check (useful for your fraud logic later)
df_claims.groupBy("INCIDENT_HOUR_OF_THE_DAY").count().orderBy("INCIDENT_HOUR_OF_THE_DAY").show(24)

# COMMAND ----------

df_claims.select("AGENT_ID").distinct().count()

# COMMAND ----------

# Check for demographic column inconsistencies for same CUSTOMER_ID
demographic_cols = [
    "CUSTOMER_NAME", "SSN", "AGE", "MARITAL_STATUS", "SOCIAL_CLASS", "RISK_SEGMENTATION"
]

# Count distinct values for each demographic column per CUSTOMER_ID
for col_name in demographic_cols:
    display(
        df_claims.groupBy("CUSTOMER_ID")
        .agg(spark_sum(col(col_name).isNotNull().cast("int")).alias("non_null_count"),
             spark_sum(col(col_name).isNull().cast("int")).alias("null_count"),
             ).filter("non_null_count > 1 or null_count > 0")
    )

# Find CUSTOMER_IDs with inconsistent demographic values
for col_name in demographic_cols:
    display(
        df_claims.groupBy("CUSTOMER_ID")
        .agg(
            spark_sum(col(col_name).isNotNull().cast("int")).alias("non_null_count"),
            spark_sum(col(col_name).isNull().cast("int")).alias("null_count"),
            ).count()
    )

# COMMAND ----------

df = spark.sql("""
    SELECT COUNT(DISTINCT POLICY_NUMBER) as distinct_policies, COUNT(*) as total_claims
    FROM claims_fraud_lakehouse.bronze.insurance_data
""")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN claims_fraud_lakehouse.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM claims_fraud_lakehouse.bronze.employee_data LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM claims_fraud_lakehouse.bronze.insurance_data LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN claims_fraud_lakehouse.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN claims_fraud_lakehouse.default;

# COMMAND ----------

