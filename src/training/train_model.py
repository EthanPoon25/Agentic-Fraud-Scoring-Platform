# Databricks notebook source
# Trains the fraud model using the Feature Store table + labels, logs
# everything to MLflow, and registers the model to Unity Catalog.

# COMMAND ----------

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_recall_curve, average_precision_score
from lightgbm import LGBMClassifier

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

fe = FeatureEngineeringClient()

labels_df = spark.table("fraud_platform.features.txn_labels")

# FeatureLookup pulls features by primary key at training time, and the exact
# same lookup happens automatically at serving time - this is what prevents
# training/serving skew.
from databricks.feature_engineering import FeatureLookup

feature_lookups = [
    FeatureLookup(
        table_name="fraud_platform.features.txn_features",
        lookup_key="TransactionID",
    )
]

training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=feature_lookups,
    label="isFraud",
    exclude_columns=["TransactionID"],
)

training_df = training_set.load_df().toPandas()
print(f"Training set: {training_df.shape}")

# COMMAND ----------

X = training_df.drop(columns=["isFraud"])
y = training_df["isFraud"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# COMMAND ----------

with mlflow.start_run(run_name="fraud_lgbm") as run:
    model = LGBMClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        class_weight="balanced",  # IEEE-CIS is heavily imbalanced (~3.5% fraud)
        random_state=42,
    )
    model.fit(X_train, y_train)

    probs = model.predict_proba(X_test)[:, 1]
    test_auc = roc_auc_score(y_test, probs)
    test_ap = average_precision_score(y_test, probs)  # more informative than AUC on imbalanced data

    mlflow.log_param("n_estimators", 300)
    mlflow.log_param("max_depth", 6)
    mlflow.log_metric("test_auc", test_auc)
    mlflow.log_metric("test_avg_precision", test_ap)

    print(f"Test AUC: {test_auc:.4f}")
    print(f"Test Average Precision: {test_ap:.4f}")

    # Log via the Feature Engineering client, not plain mlflow.sklearn -
    # this bundles the feature lookups into the model so serving can fetch
    # features automatically by TransactionID rather than requiring the
    # caller to supply every feature by hand.
    fe.log_model(
        model=model,
        artifact_path="fraud_model",
        flavor=mlflow.lightgbm,
        training_set=training_set,
        registered_model_name="fraud_platform.gold.fraud_model",
    )

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")
