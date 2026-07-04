# Databricks notebook source
# A minimal eval harness. Build a small labeled set (20-30 rows is enough)
# where you've written down, in your own words, what the "correct" case
# summary should conclude (flag / escalate / false positive, and the key
# risk factor). Then check whether the agent's output agrees.
#
# This is the artifact that proves you evaluated the agent instead of just
# shipping whatever it produced - put a screenshot of the results table in
# your README.

# COMMAND ----------

from agent import investigate  # if running as a notebook, inline the function instead

# Your hand-labeled ground truth. Build this by manually reviewing ~20-30
# flagged transactions yourself first.
eval_set = [
    {"transaction_id": 2987000, "expected_verdict": "escalate", "expected_reason": "card testing pattern"},
    {"transaction_id": 2987441, "expected_verdict": "flag", "expected_reason": "shared address with many cards"},
    {"transaction_id": 3012209, "expected_verdict": "false positive", "expected_reason": "consistent seasonal purchase"},
    # ... add more rows here as you build out your labeled set
]

# COMMAND ----------

results = []
for case in eval_set:
    summary = investigate(case["transaction_id"])
    # naive check: did the agent's recommended action mention the expected verdict?
    verdict_match = case["expected_verdict"].lower() in summary.lower()
    results.append({
        "transaction_id": case["transaction_id"],
        "expected_verdict": case["expected_verdict"],
        "verdict_match": verdict_match,
        "agent_summary": summary,
    })

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(results)
accuracy = results_df["verdict_match"].mean()
print(f"Verdict agreement rate: {accuracy:.1%} ({results_df['verdict_match'].sum()}/{len(results_df)})")
display(results_df)

# Save for your writeup
results_df.to_csv("/tmp/agent_eval_results.csv", index=False)
