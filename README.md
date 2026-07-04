# Real-Time Fraud Detection + Agentic Investigation Platform (Databricks)

An end-to-end lakehouse system: streaming ingestion → Delta Live Tables →
graph enrichment → real-time model serving → an LLM agent that investigates
flagged transactions → a Databricks App for human review.

Built on the IEEE-CIS Fraud Detection dataset (Kaggle) replayed as a live
stream, with synthetic policy docs and case notes powering the agent's
retrieval layer.

## Architecture

```
Kafka/CSV replay --> Auto Loader --> DLT (bronze/silver/gold)
                                         |
                            +------------+------------+
                            |                         |
                     Feature Store            Graph enrichment
                     (behavioral agg)         (GraphFrames)
                            |                         |
                            +------------+------------+
                                         |
                             Real-time model serving
                                         |
                              risk score > threshold?
                                         |
                              Agent investigation
                        (Vector Search + Mosaic AI Agent)
                                         |
                             Databricks App (analyst UI)
```

## Repo layout

```
databricks.yml              # Databricks Asset Bundle config (deploy everything)
resources/                  # DAB resource definitions (pipelines, jobs, endpoints)
src/streaming/producer.py   # Replays historical rows as a live stream
src/dlt/pipeline.py         # DLT medallion pipeline (bronze/silver/gold)
src/graph/graph_enrichment.py  # GraphFrames connected-components fraud rings
src/features/build_features.py # Feature Store table creation
src/training/train_model.py    # Trains + registers the fraud model with MLflow
src/serving/deploy_endpoint.py # Deploys real-time serving endpoint
src/agent/build_vector_index.py # Embeds policy docs + case notes
src/agent/agent.py              # The investigation agent (tool-calling)
src/app/app.py                  # Databricks App: analyst review UI (Streamlit)
docs_sample/                    # Synthetic policy docs + case notes to embed
notebooks/load_bronze.py        # One-time notebook: CSV -> bronze table
```

## Prerequisites

1. A Databricks workspace (Free Edition is enough): https://www.databricks.com/learn/free-edition
2. Databricks CLI installed and authenticated: `pip install databricks-cli` then `databricks configure`
3. IEEE-CIS Fraud Detection dataset downloaded from Kaggle (`train_transaction.csv`)
4. Python 3.10+, and `pip install databricks-sdk mlflow lightgbm graphframes-spark` locally if you want to run anything outside the workspace

## Step-by-step run order

1. **Load raw data.** Open `notebooks/load_bronze.py` in your workspace, update the CSV path, run it. Creates `fraud_platform.bronze.transactions_raw`.
2. **Deploy the bundle.** From the repo root:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   ```
   This creates the Unity Catalog schemas, the DLT pipeline, and the training/serving jobs as real objects in your workspace, defined declaratively instead of clicked together by hand.
3. **Run the DLT pipeline.** In the workspace UI: Workflows → Pipelines → `fraud_dlt_pipeline` → Start. Or: `databricks bundle run fraud_dlt_pipeline -t dev`.
4. **Run graph enrichment.** `databricks bundle run graph_enrichment_job -t dev`
5. **Build features + train the model.** `databricks bundle run train_fraud_model_job -t dev`
6. **Deploy the serving endpoint.** Run `src/serving/deploy_endpoint.py` (one-time; endpoints aren't bundle resources in every workspace tier).
7. **Build the vector index for the agent.** `databricks bundle run build_vector_index_job -t dev`
8. **Try the agent** interactively: open `src/agent/agent.py` as a notebook and run the example at the bottom.
9. **Launch the analyst app.** Deploy `src/app/app.py` as a Databricks App (Compute → Apps → Create App → point at this file).

## What to put in your portfolio writeup

- Screenshot of the DLT lineage graph
- `test_auc` from the MLflow run + serving endpoint p50/p99 latency (see `src/serving/deploy_endpoint.py` load test snippet)
- Size/count of the largest connected component the graph job flags
- 3-5 example agent case summaries next to your own "ground truth" summary, to show you evaluated it rather than just shipping it
