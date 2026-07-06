# Databricks notebook source
# Builds the Feature Store table used for both training and real-time
# scoring, joining gold transaction data with the graph features.

# COMMAND ----------



# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.functions import col

fe = FeatureEngineeringClient()

# COMMAND ----------

txns = spark.table("fraud_platform.silver.gold_transactions_enriched")
graph_feats = spark.table("fraud_platform.gold.card_graph_features")

feature_df = (
    txns.join(graph_feats, on="card1", how="left")
    .fillna({"component_size": 1})  # cards with no graph edges are their own component of size 1
    .select(
        "TransactionID",
        "card1",
        "TransactionAmt",
        "avg_amt",
        "stddev_amt",
        "component_size",
        "isFraud",  # label, only present in training data - drop before serving features
    )
)

# COMMAND ----------

# create_table only needs to run once; on subsequent runs use fe.write_table
# in "merge" mode to upsert new rows.
try:
    fe.create_table(
        name="fraud_platform.features.txn_features",
        primary_keys=["TransactionID"],
        df=feature_df.drop("isFraud"),
        description="Behavioral aggregates + graph connected-component size per transaction",
    )
    print("Created feature table.")
except Exception as e:
    print(f"Table may already exist, upserting instead: {e}")
    fe.write_table(
        name="fraud_platform.features.txn_features",
        df=feature_df.drop("isFraud"),
        mode="merge",
    )

# COMMAND ----------

# Separately persist labels for training (kept out of the feature table since
# labels shouldn't be available at serving time).
(
    feature_df.select("TransactionID", "isFraud")
    .write.mode("overwrite")
    .saveAsTable("fraud_platform.features.txn_labels")
)

print("Wrote labels table.")