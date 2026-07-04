# Databricks notebook source
# The investigation agent. Given a flagged TransactionID, this:
#   1. Pulls the transaction + its features
#   2. Retrieves relevant policy/case context via Vector Search
#   3. Retrieves related accounts via the graph table
#   4. Calls an LLM to synthesize a structured case summary
#
# This is deliberately written as plain, readable tool-calling code rather
# than a heavier agent framework, so it's easy to follow and to evaluate.
# Swap the manual orchestration below for Mosaic AI Agent Framework /
# LangGraph once you're comfortable with what each step is doing.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import json

vsc = VectorSearchClient()
w = WorkspaceClient()

VS_ENDPOINT = "fraud_vs_endpoint"
VS_INDEX = "fraud_platform.docs.policy_index"
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"  # swap for whichever chat endpoint your workspace has


# COMMAND ----------

def get_transaction_context(transaction_id: int) -> dict:
    """Tool 1: pull the transaction row + its engineered features."""
    row = (
        spark.table("fraud_platform.features.txn_features")
        .filter(f"TransactionID = {transaction_id}")
        .collect()
    )
    if not row:
        return {}
    return row[0].asDict()


def get_related_accounts(card1: int) -> dict:
    """Tool 2: pull the graph component this card belongs to."""
    row = (
        spark.table("fraud_platform.gold.card_graph_features")
        .filter(f"card1 = {card1}")
        .collect()
    )
    if not row:
        return {"component_id": None, "component_size": 1}
    return row[0].asDict()


def retrieve_relevant_docs(query_text: str, num_results: int = 3) -> list:
    """Tool 3: vector search over policy docs + past case notes."""
    results = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX).similarity_search(
        query_text=query_text,
        columns=["doc_name", "chunk_text"],
        num_results=num_results,
    )
    hits = results.get("result", {}).get("data_array", [])
    return [{"doc_name": h[0], "text": h[1]} for h in hits]


# COMMAND ----------

def build_investigation_prompt(txn: dict, graph: dict, docs: list) -> str:
    docs_block = "\n\n".join(f"[{d['doc_name']}]: {d['text']}" for d in docs)

    return f"""You are a fraud investigation assistant helping a human compliance
analyst triage a flagged transaction. Use only the information provided below.
Do not invent facts not present in the context.

TRANSACTION:
{json.dumps(txn, default=str, indent=2)}

GRAPH CONTEXT:
{json.dumps(graph, default=str, indent=2)}

RELEVANT POLICY AND PAST CASES:
{docs_block}

Write a structured case summary with exactly these sections:
1. Risk factors observed (bullet points, grounded in the transaction/graph data above)
2. Closest matching pattern from policy/case history (name it, one sentence why)
3. Recommended action (flag / escalate / likely false positive - pick one, with reasoning)

Keep it under 150 words. Be direct and specific with numbers where you have them."""


def investigate(transaction_id: int) -> str:
    txn = get_transaction_context(transaction_id)
    if not txn:
        return f"No transaction found with ID {transaction_id}"

    graph = get_related_accounts(txn["card1"])

    query = (
        f"transaction amount {txn.get('TransactionAmt')} vs average "
        f"{txn.get('avg_amt')}, graph component size {graph.get('component_size')}"
    )
    docs = retrieve_relevant_docs(query)

    prompt = build_investigation_prompt(txn, graph, docs)

    response = w.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=400,
    )
    return response.choices[0].message.content


# COMMAND ----------

# Example usage - replace with a real flagged TransactionID from your data
if __name__ == "__main__":
    example_id = 2987000
    summary = investigate(example_id)
    print(summary)
