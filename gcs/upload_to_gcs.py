from google.cloud import storage
import os

PROJECT_ID = "fraud-analytics-gcp"
BUCKET_NAME = "fraud-analytics-gcp-data"
DATA_DIR = "data"  # folder where your CSVs live

FILES = [
    "transactions_data.csv",
    "cards_data.csv",
    "users_data.csv",
    "mcc_codes.csv",
    "train_fraud_labels.csv",
]

def upload_file(bucket, local_path, gcs_path):
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} → gs://{BUCKET_NAME}/{gcs_path}")

def main():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    for filename in FILES:
        local_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(local_path):
            upload_file(bucket, local_path, f"raw/{filename}")
        else:
            print(f"WARNING: {local_path} not found, skipping")

if __name__ == "__main__":
    main()
