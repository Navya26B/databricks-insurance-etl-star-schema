# Databricks notebook source
 
dbutils.widgets.text("catalog_name", "claims_fraud_lakehouse", "Catalog Name")
dbutils.widgets.text("source_schema", "raw", "Source Schema (Raw Layer)")
dbutils.widgets.text("source_volume", "raw_data", "Source Volume Name")
dbutils.widgets.text("target_schema", "bronze", "Target Schema (Bronze Layer")
dbutils.widgets.text("source_table", "claims_raw", "Source Table Name")
dbutils.widgets.text("target_table", "claims_bronze", "Target Table Name")
 
catalog_name = dbutils.widgets.get("catalog_name")
source_schema = dbutils.widgets.get("source_schema")
source_volume = dbutils.widgets.get("source_volume")
target_schema = dbutils.widgets.get("target_schema")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------

print(f"Config: catalog_name={catalog_name}, source_schema={source_schema}, "
      f"source_volume={source_volume}, target_schema={target_schema}")

# COMMAND ----------

# Validate required parameters before proceeding to any streaming or file operations
required_params = {
    "catalog_name": catalog_name,
    "source_schema": source_schema,
    "source_volume": source_volume,
    "target_schema": target_schema,
    "source_table": source_table,
    "target_table": target_table
}
missing = [k for k, v in required_params.items() if not v or v.strip() == ""]
if missing:
    dbutils.notebook.exit(f"Missing required parameters: {', '.join(missing)}. "
                          "Please set all notebook widgets before proceeding.")
else:
    print("All required parameters are set.")

# COMMAND ----------

# Configure paths for source and target locations
source_path = f"/Volumes/{catalog_name}/{source_schema}/{source_volume}/{source_table}"
target_table_name = f"{catalog_name}.{target_schema}.{target_table}"  # Delta table in bronze schema
checkpoint_path = f"/Volumes/{catalog_name}/{source_schema}/{source_volume}/_checkpoints/{target_table}"
schema_location = f"/Volumes/{catalog_name}/{source_schema}/{source_volume}/_schemas/{target_table}"

print(f"Source Path: {source_path}")
print(f"Target Table: {target_table_name}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Schema Location: {schema_location}")

# COMMAND ----------

# DBTITLE 1,Validate Source Data
# Validate if source file exists
file_path = f"{source_path}/{source_table}.csv"
try:
    dbutils.fs.head(file_path, 1)
    print(f"✓ Validation passed: File exists at {file_path}")
except Exception as e:
    print(f"❌ File does not exist: {file_path}")
    dbutils.notebook.exit(f"Source file not found: {file_path}")


# COMMAND ----------

# DBTITLE 1,Auto Loader Streaming
# Auto Loader: Stream CSV files from raw to bronze with schema evolution
print("Starting Auto Loader stream...")

# Read streaming data using Auto Loader (cloudFiles)
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", schema_location)
  .option("header", "true")
  .option("inferSchema", "true")
  .load(source_path)
)

print("Auto Loader stream configured. Starting write to Delta table...")

# Write stream to Delta table with schema evolution enabled
query = (df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")  # Enable schema evolution
  .trigger(availableNow=True)  # Process all available files then stop
  .toTable(target_table_name)
)

print(f"✓ Stream started successfully!")
print(f"  Stream ID: {query.id}")
print(f"  Status: {query.status}")
print(f"\nWaiting for stream to complete...")

# Wait for the stream to complete
query.awaitTermination()

print(f"\n✓ Stream completed! Data loaded to {target_table_name}")

# COMMAND ----------



# COMMAND ----------

