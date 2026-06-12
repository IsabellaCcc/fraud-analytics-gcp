"""
Fraud Analytics Pipeline DAG
=============================
Orchestrates the end-to-end GCP data pipeline for the banking fraud
analytics project:

    upload_to_gcs  ->  load_raw_to_bq  ->  build_optimized_table  ->  validate_fraud_rate

This is the orchestration layer that ties together the GCS data lake and
the BigQuery warehouse built in earlier phases. It mirrors the manual
steps run by hand, now as a scheduled, observable, retryable pipeline.

Local Airflow notes:
- Auth uses the GOOGLE_APPLICATION_CREDENTIALS env var (service-account key).
- Set in the same shell that runs `airflow standalone`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
PROJECT_ID = "fraud-analytics-gcp"
BUCKET = "fraud-analytics-gcp-data"
DATASET = "fraud_analytics"

# Local data directory (absolute path so Airflow finds it regardless of CWD)
DATA_DIR = os.path.expanduser("~/fraud-analytics-gcp/data")

# (local_filename, gcs_object_path, bq_table_name)
TABLES = [
    ("transactions_data.csv", "raw/transactions_data.csv", "transactions"),
    ("cards_data.csv", "raw/cards_data.csv", "cards"),
    ("users_data.csv", "raw/users_data.csv", "users"),
    ("mcc_codes.csv", "raw/mcc_codes.csv", "mcc_codes"),
    ("train_fraud_labels.csv", "raw/train_fraud_labels.csv", "fraud_labels"),
]

default_args = {
    "owner": "isabella",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ----------------------------------------------------------------------
# Python callable for the final validation step
# ----------------------------------------------------------------------
def validate_fraud_rate(**context):
    """Run the baseline fraud-rate query and assert it matches the
    expected ~0.15% from the original DuckDB project. Fails the task
    (and the DAG run) if the migrated data drifts."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
          COUNT(*) AS total_txns,
          COUNTIF(f.is_fraud) AS fraud_txns,
          ROUND(100 * COUNTIF(f.is_fraud) / COUNT(*), 4) AS fraud_rate_pct
        FROM `{PROJECT_ID}.{DATASET}.transactions_optimized` AS t
        JOIN `{PROJECT_ID}.{DATASET}.fraud_labels` AS f
          ON t.id = f.transaction_id
    """
    row = list(client.query(query).result())[0]
    print(
        f"Validation: total={row.total_txns:,} "
        f"fraud={row.fraud_txns:,} rate={row.fraud_rate_pct}%"
    )

    # Sanity check: fraud rate should be in a plausible band
    if not (0.10 <= row.fraud_rate_pct <= 0.20):
        raise ValueError(
            f"Fraud rate {row.fraud_rate_pct}% outside expected 0.10-0.20% band "
            "- data may have loaded incorrectly."
        )
    print("Fraud rate within expected band - pipeline validated.")


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="fraud_analytics_pipeline",
    description="End-to-end GCP fraud analytics pipeline (GCS -> BigQuery)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,          # trigger manually
    catchup=False,
    tags=["gcp", "bigquery", "fraud", "portfolio"],
) as dag:

    # --- Stage 1: upload each local CSV to GCS ------------------------
    upload_tasks = []
    for local_name, gcs_path, _ in TABLES:
        upload = LocalFilesystemToGCSOperator(
            task_id=f"upload_{local_name.replace('.csv', '')}",
            src=os.path.join(DATA_DIR, local_name),
            dst=gcs_path,
            bucket=BUCKET,
        )
        upload_tasks.append(upload)

    # --- Stage 2: load each GCS file into a raw BigQuery table --------
    load_tasks = []
    for _, gcs_path, table in TABLES:
        load = BigQueryInsertJobOperator(
            task_id=f"load_{table}",
            configuration={
                "load": {
                    "sourceUris": [f"gs://{BUCKET}/{gcs_path}"],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET,
                        "tableId": table,
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "autodetect": True,
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )
        load_tasks.append(load)

    # --- Stage 3: build the partitioned + clustered optimized table ---
    build_optimized = BigQueryInsertJobOperator(
        task_id="build_optimized_table",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                      `{PROJECT_ID}.{DATASET}.transactions_optimized`
                    PARTITION BY DATE(date)
                    CLUSTER BY mcc, merchant_state
                    AS
                    SELECT
                      id,
                      CAST(date AS TIMESTAMP) AS date,
                      client_id, card_id, amount, use_chip,
                      merchant_id, merchant_city, merchant_state,
                      zip, mcc, errors
                    FROM `{PROJECT_ID}.{DATASET}.transactions`
                """,
                "useLegacySql": False,
            }
        },
    )

    # --- Stage 4: validate the migrated data --------------------------
    validate = PythonOperator(
        task_id="validate_fraud_rate",
        python_callable=validate_fraud_rate,
    )

    # --- Dependencies -------------------------------------------------
    # Each upload feeds its matching load; all loads must finish before
    # the optimized table is built; validation runs last.
    for upload, load in zip(upload_tasks, load_tasks):
        upload >> load

    load_tasks >> build_optimized >> validate
