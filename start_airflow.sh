#!/bin/bash
export AIRFLOW_HOME=~/fraud-analytics-gcp/airflow
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{"conn_type": "google_cloud_platform", "extra": {"key_path": "/Users/isabella/fraud-analytics-gcp/airflow/gcp-key.json", "project": "fraud-analytics-gcp"}}'
source ~/fraud-analytics-gcp/airflow-venv/bin/activate
airflow standalone
