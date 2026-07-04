"""
Analyst review UI, deployable as a Databricks App.

Deploy via: Compute -> Apps -> Create App -> point at this file
(Databricks Apps auto-detects Streamlit apps from app.py + requirements.txt
in the same folder - see app_requirements.txt alongside this file).
"""

import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Fraud Case Review", layout="wide")

w = WorkspaceClient()


@st.cache_data(ttl=60)
def load_flagged_transactions():
    # In a real deployment, query via Databricks SQL connector or the
    # Statement Execution API. Placeholder query shown here.
    statement = w.statement_execution.execute_statement(
        warehouse_id=st.secrets.get("WAREHOUSE_ID", ""),
        statement="""
            SELECT TransactionID, card1, TransactionAmt, avg_amt, component_size
            FROM fraud_platform.features.txn_features
            WHERE TransactionAmt > avg_amt * 2
            LIMIT 50
        """,
    )
    rows = statement.result.data_array or []
    columns = [c.name for c in statement.manifest.schema.columns]
    return pd.DataFrame(rows, columns=columns)


st.title("Fraud case review")
st.caption("Flagged transactions awaiting analyst decision")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Queue")
    try:
        df = load_flagged_transactions()
    except Exception as e:
        st.warning(f"Could not load live queue ({e}). Showing a placeholder row for demo purposes.")
        df = pd.DataFrame([
            {"TransactionID": 2987000, "card1": 13553, "TransactionAmt": 340.0,
             "avg_amt": 42.1, "component_size": 12}
        ])

    selected_id = st.selectbox("Select a transaction", df["TransactionID"].tolist())
    st.dataframe(df, use_container_width=True)

with col2:
    st.subheader("Investigation")
    row = df[df["TransactionID"] == selected_id].iloc[0]

    st.metric("Transaction amount", f"${row['TransactionAmt']:.2f}")
    st.metric("Card average amount", f"${row['avg_amt']:.2f}")
    st.metric("Graph component size", int(row["component_size"]))

    if st.button("Run agent investigation"):
        with st.spinner("Retrieving context and generating summary..."):
            try:
                # Calls the LLM serving endpoint directly - mirrors src/agent/agent.py
                # logic; in production, factor this into a shared module instead
                # of duplicating it here.
                from src.agent.agent import investigate
                summary = investigate(int(selected_id))
            except Exception as e:
                summary = f"(Agent call failed in this demo environment: {e})"
        st.text_area("Agent case summary", summary, height=250)

    decision = st.radio("Analyst decision", ["Pending", "Approve as fraud", "Dismiss as false positive"])
    if st.button("Submit decision"):
        st.success(f"Recorded: {decision} for transaction {selected_id}")
        # In production: write this decision back to a Delta table for
        # audit trail and to eventually retrain/calibrate the model.
