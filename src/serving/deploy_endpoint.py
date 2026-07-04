"""
Deploys the registered fraud model to a real-time Model Serving endpoint,
and includes a small load test to measure latency for your writeup.

Run this from your local machine (with the Databricks SDK configured) or
as a one-off notebook cell in the workspace.
"""

import time
import statistics
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

ENDPOINT_NAME = "fraud-scoring"
MODEL_NAME = "fraud_platform.gold.fraud_model"
MODEL_VERSION = "1"  # bump this after retraining


def deploy():
    w = WorkspaceClient()

    config = EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=MODEL_VERSION,
                workload_size="Small",
                scale_to_zero_enabled=True,
            )
        ]
    )

    existing = [e.name for e in w.serving_endpoints.list()]
    if ENDPOINT_NAME in existing:
        print(f"Endpoint {ENDPOINT_NAME} exists, updating config...")
        w.serving_endpoints.update_config(name=ENDPOINT_NAME, served_entities=config.served_entities)
    else:
        print(f"Creating endpoint {ENDPOINT_NAME}...")
        w.serving_endpoints.create(name=ENDPOINT_NAME, config=config)

    print("Deployment triggered. Check Serving tab in the UI for readiness.")


def load_test(sample_rows: list, n_requests: int = 100):
    """
    sample_rows: list of dicts, each a TransactionID (or full feature row,
    depending on how you configured the endpoint's expected input) to score.
    """
    w = WorkspaceClient()
    latencies = []

    for i in range(n_requests):
        row = sample_rows[i % len(sample_rows)]
        start = time.perf_counter()
        w.serving_endpoints.query(name=ENDPOINT_NAME, dataframe_records=[row])
        latencies.append((time.perf_counter() - start) * 1000)  # ms

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p99 = latencies[int(len(latencies) * 0.99) - 1]

    print(f"Requests: {n_requests}")
    print(f"p50 latency: {p50:.1f} ms")
    print(f"p99 latency: {p99:.1f} ms")
    print(f"mean latency: {statistics.mean(latencies):.1f} ms")
    return {"p50_ms": p50, "p99_ms": p99}


if __name__ == "__main__":
    deploy()
    # After the endpoint is ready (check the UI), run something like:
    # sample = [{"TransactionID": 2987000}, {"TransactionID": 2987001}]
    # load_test(sample, n_requests=200)
