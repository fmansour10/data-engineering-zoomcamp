import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, Forbidden
import time

# Change this to your bucket name
BUCKET_NAME = "data-engineering-485022-module-4"

# If you authenticated through the GCP SDK you can comment out these two lines
# CREDENTIALS_FILE = "gcs.json"
# client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
client = storage.Client(project='data-engineering-485022')

PROJECT_ID = "data-engineering-485022"
DATASET = "trips_data_all"
TABLE = "fhv_tripdata_2019"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

MONTHS = [f"{m:02d}" for m in range(1, 13)]
FILES = [f"fhv_tripdata_2019-{month}.csv.gz" for month in MONTHS]


def download_and_upload(filename):
    url = f"{BASE_URL}/{filename}"
    local_path = f"/tmp/{filename}"

    print(f"[{filename}] Downloading...")
    try:
        urllib.request.urlretrieve(url, local_path)
    except Exception as e:
        print(f"[{filename}] Download failed: {e}")
        return None

    print(f"[{filename}] Uploading to GCS...")
    try:
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"fhv/{filename}")
        blob.upload_from_filename(local_path)
        print(f"[{filename}] Uploaded to gs://{BUCKET_NAME}/fhv/{filename}")
    except Forbidden as e:
        print(f"[{filename}] Permission denied: {e}")
        return None
    except Exception as e:
        print(f"[{filename}] GCS upload failed: {e}")
        return None
    finally:
        os.remove(local_path)

    return f"gs://{BUCKET_NAME}/fhv/{filename}"


def load_to_bigquery(gcs_uris):
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"\nLoading {len(gcs_uris)} files into BigQuery table {table_ref}...")
    job = bq_client.load_table_from_uri(gcs_uris, table_ref, job_config=job_config)

    while not job.done():
        print("  Waiting for BigQuery load job...")
        time.sleep(5)

    if job.errors:
        print(f"BigQuery load failed: {job.errors}")
    else:
        table = bq_client.get_table(table_ref)
        print(f"Done! {table.num_rows:,} rows in {table_ref}")


if __name__ == "__main__":
    if not BUCKET_NAME:
        print("Error: BUCKET_NAME is not set.")
        sys.exit(1)

    # Download and upload all files concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(download_and_upload, FILES))

    gcs_uris = [r for r in results if r is not None]

    if not gcs_uris:
        print("No files were successfully uploaded. Exiting.")
        sys.exit(1)

    # Load all files into BigQuery in a single job
    load_to_bigquery(gcs_uris)