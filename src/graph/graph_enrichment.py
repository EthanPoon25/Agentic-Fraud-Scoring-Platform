# Databricks notebook source
# Graph enrichment job - runs on serverless compute, so it avoids GraphFrames
# (which needs a JVM package that serverless doesn't support) and instead
# implements connected components directly with PySpark using iterative
# label propagation. Same goal as GraphFrames' connectedComponents(): find
# clusters of cards/addresses/email domains that are unusually interlinked,
# which is how fraud rings show up.
#
# How label propagation works, in plain terms: every node starts labeled
# with its own ID. On each round, every node looks at its neighbors' labels
# and adopts the smallest one it sees. Nodes that are connected (directly or
# through a chain of other nodes) converge on the same label after enough
# rounds - that shared label IS the component ID.

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, min as spark_min

CATALOG = "fraud_platform"
MAX_ITERATIONS = 12  # enough for small/medium graphs; increase if labels haven't converged (see printed counts below)

# COMMAND ----------

txns = spark.table(f"{CATALOG}.gold.gold_transactions_enriched")

# Build edges: card <-> address, card <-> email domain. Same idea as the
# GraphFrames version - a card connects to whatever address/email it was
# used with, and shared addresses/emails across many cards is the fraud-ring
# signal.
edges_addr = txns.select(
    concat(lit("card_"), col("card1")).alias("src"),
    concat(lit("addr_"), col("addr1")).alias("dst"),
).na.drop()

edges_email = txns.select(
    concat(lit("card_"), col("card1")).alias("src"),
    concat(lit("email_"), col("P_emaildomain")).alias("dst"),
).na.drop()

edges = edges_addr.union(edges_email).distinct()

# Make edges undirected: if A connects to B, B also connects to A.
edges_undirected = edges.union(edges.select(col("dst").alias("src"), col("src").alias("dst"))).distinct()

edges_undirected.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold._tmp_edges")
edges_undirected = spark.table(f"{CATALOG}.gold._tmp_edges")  # reread to cut lineage

# COMMAND ----------

# Initialize: every node's label is itself.
all_nodes = (
    edges_undirected.select(col("src").alias("id"))
    .union(edges_undirected.select(col("dst").alias("id")))
    .distinct()
)
labels = all_nodes.withColumn("label", col("id"))
labels.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold._tmp_labels")

# COMMAND ----------

for i in range(MAX_ITERATIONS):
    labels = spark.table(f"{CATALOG}.gold._tmp_labels")

    # Each node looks at its neighbors' current labels...
    neighbor_labels = (
        edges_undirected.join(labels, edges_undirected.dst == labels.id)
        .select(edges_undirected.src.alias("id"), labels.label.alias("candidate_label"))
    )
    # ...and also considers its own current label, then takes the minimum.
    own_labels = labels.select(col("id"), col("label").alias("candidate_label"))

    new_labels = (
        neighbor_labels.union(own_labels)
        .groupBy("id")
        .agg(spark_min("candidate_label").alias("label"))
    )

    # Write to a fresh table each round instead of reusing the same one -
    # this truncates Spark's query lineage so it doesn't grow unboundedly
    # across iterations (a common gotcha with iterative Spark jobs).
    new_labels.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold._tmp_labels_next")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.gold._tmp_labels")
    spark.sql(f"ALTER TABLE {CATALOG}.gold._tmp_labels_next RENAME TO {CATALOG}.gold._tmp_labels")

    distinct_label_count = spark.table(f"{CATALOG}.gold._tmp_labels").select("label").distinct().count()
    print(f"Iteration {i+1}: {distinct_label_count} distinct components so far")

# COMMAND ----------

final_labels = spark.table(f"{CATALOG}.gold._tmp_labels")

component_sizes = (
    final_labels.groupBy("label")
    .count()
    .withColumnRenamed("count", "component_size")
    .withColumnRenamed("label", "component_id")
)

card_components = (
    final_labels.filter(col("id").startswith("card_"))
    .join(component_sizes, final_labels.label == component_sizes.component_id)
    .withColumn("card1", col("id").substr(6, 100).cast("long"))
    .select("card1", "component_id", "component_size")
)

card_components.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.card_graph_features")

# Clean up temp tables
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.gold._tmp_edges")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.gold._tmp_labels")

# COMMAND ----------

print("Largest connected components (potential fraud rings):")
display(
    card_components.select("component_id", "component_size")
    .distinct()
    .orderBy(col("component_size").desc())
    .limit(20)
)