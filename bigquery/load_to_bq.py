from google.cloud import bigquery

PROJECT_ID = "fraud-analytics-gcp"
DATASET = "fraud_analytics"
BUCKET = "fraud-analytics-gcp-data"

client = bigquery.Client(project=PROJECT_ID)

# Tables to load: (table_name, gcs_filename)
TABLES = [
    ("transactions", "transactions_data.csv"),
    ("cards", "cards_data.csv"),
    ("users", "users_data.csv"),
    ("mcc_codes", "mcc_codes.csv"),
    ("fraud_labels", "train_fraud_labels.csv"),
]

def load_table(table_name, filename):
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    uri = f"gs://{BUCKET}/raw/{filename}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,  # let BQ infer schema for now
        write_disposition="WRITE_TRUNCATE",  # overwrite if exists
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # wait for completion

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows:,} rows into {table_name}")

def main():
    for table_name, filename in TABLES:
        load_table(table_name, filename)

if __name__ == "__main__":
    main()
