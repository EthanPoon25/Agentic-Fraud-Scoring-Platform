# Databricks notebook source
# This file is the DLT pipeline source. Do NOT run it directly with Run All -
# point a DLT Pipeline object at this file instead (see README step 3, or
# resources/dlt_pipeline.yml if deploying via Asset Bundle).

import dlt
from pyspark.sql.functions import col, count, avg, stddev, window

# ---------------------------------------------------------------------------
# BRONZE
# ---------------------------------------------------------------------------
# If you're using the Kafka producer (src/streaming/producer.py), swap this
# table definition for a Kafka read:
#
# @dlt.table(name="bronze_transactions")
# def bronze_transactions():
#     return (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", "<broker>")
#         .option("subscribe", "transactions")
#         .load()
#         .selectExpr("CAST(value AS STRING) as json")
#         .select(from_json(col("json"), transaction_schema).alias("data"))
#         .select("data.*")
#     )
#
# Otherwise, this simpler version streams off the bronze table you created
# in notebooks/load_bronze.py, which is enough to prove the pipeline mechanics.


@dlt.table(
    name="bronze_transactions",
    comment="Raw transactions, streamed incrementally from the landing table",
)
def bronze_transactions():
    return spark.readStream.table("fraud_platform.bronze.transactions_raw")


# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
@dlt.table(
    name="silver_transactions",
    comment="Cleaned transactions with data quality expectations applied",
)
@dlt.expect_or_drop("valid_amount", "TransactionAmt > 0")
@dlt.expect_or_drop("valid_card", "card1 IS NOT NULL")
@dlt.expect("has_email_domain", "P_emaildomain IS NOT NULL")  # warn only, don't drop
def silver_transactions():
    return (
        dlt.read_stream("bronze_transactions")
        .withColumn("event_time", col("event_time").cast("timestamp"))
        .dropDuplicates(["TransactionID"])
    )


# ---------------------------------------------------------------------------
# GOLD
# ---------------------------------------------------------------------------
@dlt.table(
    name="gold_card_behavior",
    comment="Per-card rolling behavioral aggregates, the basis for model features",
)
def gold_card_behavior():
    return (
        dlt.read("silver_transactions")
        .groupBy("card1")
        .agg(
            count("TransactionID").alias("txn_count"),
            avg("TransactionAmt").alias("avg_amt"),
            stddev("TransactionAmt").alias("stddev_amt"),
        )
    )


@dlt.table(
    name="gold_transactions_enriched",
    comment="Transaction-level table joined with card behavioral features - "
             "this is what feeds the feature store and the graph job",
)
def gold_transactions_enriched():
    txns = dlt.read("silver_transactions")
    behavior = dlt.read("gold_card_behavior")
    return txns.join(behavior, on="card1", how="left")
