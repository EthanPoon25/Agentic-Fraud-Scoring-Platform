# Databricks notebook source
# Graph enrichment job. Run this as a scheduled Lakeflow Job (see
# resources/jobs.yml), separate from the real-time scoring path, since graph
# computation is too slow for a per-transaction latency budget. It runs
# periodically and writes a `component_id` / `component_size` feature that
# the model reads at scoring time.

# COMMAND ----------

# On Databricks Runtime for ML, GraphFrames is preinstalled. On plain
# Runtime you may need: %pip install graphframes
from graphframes import GraphFrame
from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

txns = spark.table("fraud_platform.gold.transactions_enriched")

# Build a bipartite-ish graph: nodes are (card, address, email domain)
# identities, edges connect a card to the address/email domain seen on each
# transaction. Fraud rings show up as unusually large connected components -
# many cards funneling through the same address or email domain.

card_nodes = txns.select(concat(lit("card_"), col("card1")).alias("id")).distinct()
addr_nodes = txns.select(concat(lit("addr_"), col("addr1")).alias("id")).distinct()
email_nodes = txns.select(concat(lit("email_"), col("P_emaildomain")).alias("id")).distinct()

vertices = card_nodes.union(addr_nodes).union(email_nodes).na.drop()

edges_addr = txns.select(
    concat(lit("card_"), col("card1")).alias("src"),
    concat(lit("addr_"), col("addr1")).alias("dst"),
).na.drop()

edges_email = txns.select(
    concat(lit("card_"), col("card1")).alias("src"),
    concat(lit("email_"), col("P_emaildomain")).alias("dst"),
).na.drop()

edges = edges_addr.union(edges_email).distinct()

# COMMAND ----------

g = GraphFrame(vertices, edges)

# Checkpointing is required for connectedComponents in GraphFrames.
spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoints")

components = g.connectedComponents()

component_sizes = (
    components.groupBy("component")
    .count()
    .withColumnRenamed("count", "component_size")
)

card_components = (
    components.filter(col("id").startswith("card_"))
    .join(component_sizes, on="component")
    .withColumn("card1", col("id").substr(6, 100).cast("long"))
    .select(
        col("card1"),
        col("component").alias("component_id"),
        col("component_size"),
    )
)

# COMMAND ----------

(
    card_components.write
    .mode("overwrite")
    .saveAsTable("fraud_platform.gold.card_graph_features")
)

# Quick sanity check: what's the largest suspicious cluster?
print("Largest connected components (potential fraud rings):")
display(
    card_components.select("component_id", "component_size")
    .distinct()
    .orderBy(col("component_size").desc())
    .limit(20)
)
