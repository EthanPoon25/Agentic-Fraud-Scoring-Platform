# Databricks notebook source
# One-time load: raw IEEE-CIS train_transaction.csv -> Unity Catalog bronze table.
#
# Steps before running:
# 1. Upload train_transaction.csv to a Unity Catalog Volume, e.g.
#    /Volumes/fraud_platform/bronze/raw_files/train_transaction.csv
# 2. Update CSV_PATH below to match.
# 3. Run this whole notebook (Run All), or Shift+Enter cell by cell.

# COMMAND ----------

CSV_PATH = "/Volumes/fraud_platform/bronze/raw_files/train_transaction.csv"
CATALOG = "fraud_platform"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold", "features", "docs"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(CSV_PATH)
)

print(f"Loaded {df.count()} rows, {len(df.columns)} columns")
df.printSchema()

# COMMAND ----------

# Add a synthetic arrival timestamp so later steps can "replay" this as a stream
# in chronological order. TransactionDT in IEEE-CIS is seconds offset from an
# arbitrary reference point, not a real calendar date - we anchor it to today
# minus the dataset's time range so timestamps look realistic.
from pyspark.sql.functions import col, expr

max_dt = df.agg({"TransactionDT": "max"}).collect()[0][0]

df = df.withColumn(
    "event_time",
    expr(f"current_timestamp() - INTERVAL {max_dt} SECONDS + (TransactionDT * INTERVAL 1 SECONDS)")
)

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.transactions_raw")
)

print("Wrote fraud_platform.bronze.transactions_raw")
display(spark.table(f"{CATALOG}.bronze.transactions_raw").limit(10))
