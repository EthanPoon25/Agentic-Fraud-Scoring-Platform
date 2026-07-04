# Databricks notebook source
# 1. Loads the sample policy docs + case notes into a Delta table
# 2. Chunks them
# 3. Creates a Vector Search endpoint + Delta Sync index over them
#
# Before running: upload the docs_sample/*.md files to a UC Volume, e.g.
# /Volumes/fraud_platform/docs/raw_docs/, or use dbutils.fs.put as shown
# below for a quick inline version.

# COMMAND ----------

from pyspark.sql import Row
import uuid

# Quick inline load for the 4 sample docs (swap for a real file read if you
# add more docs later via a Volume).
sample_docs = [
    ("policy_card_testing", """Card testing occurs when a fraudster runs many small, rapid transactions
across the same card to check whether it is still active before attempting
a larger purchase. Indicators include: multiple transactions under $5 within
a short window, transactions across unrelated merchant categories, and a
single card linked to an unusually large connected-component in the
account-relationship graph. Recommended action: flag for review if a card
shows 3+ sub-$5 transactions within 10 minutes AND a graph component size
above the 95th percentile."""),
    ("policy_account_takeover", """Account takeover typically shows a sudden change in transaction behavior
relative to a card's historical average: a transaction amount several
standard deviations above the account's average, combined with a new email
domain or shipping address never seen before on that account. Recommended
action: flag for review if TransactionAmt exceeds avg_amt by more than 3x
stddev_amt AND the associated email domain does not match prior
transactions. Route to analyst review rather than auto-blocking."""),
    ("case_note_txn_2987441", """Card showed 4 transactions under $3 within 6 minutes, then a $340 purchase
14 minutes later. Graph analysis showed the card's billing address was
shared with 11 other cards created in the prior 48 hours - consistent with
a card-testing ring. Confirmed fraud after cardholder contact; card was
already reported stolen 2 days prior."""),
    ("case_note_txn_3012209", """Flagged for a transaction 4.2x the card's average amount. Investigation
found the account had two prior large purchases in the same category
(electronics) six months apart, and the shipping address matched the
cardholder's registered address on file. Cardholder confirmed the purchase
by phone. Root cause: model does not account for seasonal purchase
patterns. Resolved as false positive."""),
]

rows = [Row(chunk_id=str(uuid.uuid4()), doc_name=name, chunk_text=text) for name, text in sample_docs]
docs_df = spark.createDataFrame(rows)

# COMMAND ----------

# Enable Change Data Feed - required for a Delta Sync vector index to track updates.
(
    docs_df.write.mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable("fraud_platform.docs.policy_chunks")
)

print("Wrote fraud_platform.docs.policy_chunks")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

ENDPOINT_NAME = "fraud_vs_endpoint"

existing_endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if ENDPOINT_NAME not in existing_endpoints:
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Created endpoint {ENDPOINT_NAME}")
else:
    print(f"Endpoint {ENDPOINT_NAME} already exists")

# COMMAND ----------

INDEX_NAME = "fraud_platform.docs.policy_index"

try:
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        source_table_name="fraud_platform.docs.policy_chunks",
        index_name=INDEX_NAME,
        pipeline_type="TRIGGERED",
        primary_key="chunk_id",
        embedding_source_column="chunk_text",
        embedding_model_endpoint_name="databricks-bge-large-en",
    )
    print(f"Created index {INDEX_NAME}")
except Exception as e:
    print(f"Index may already exist: {e}")
    idx = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
    idx.sync()
    print("Triggered sync on existing index")
