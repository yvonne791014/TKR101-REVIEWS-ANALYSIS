from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from google.cloud import bigquery, storage
import json
import tempfile
import os

PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"
REVIEWS_TABLE_ID = "test-table"
BUCKET_NAME = "my-airflow-data-bucket-2026"

BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{REVIEWS_TABLE_ID}"
GCS_TEST_PATH = "test/bq_storage_permission_test.json"


def test_bigquery():
    print("=== Testing BigQuery ===")

    client = bigquery.Client(project=PROJECT_ID)

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(BQ_TABLE, schema=schema)

    client.create_table(table, exists_ok=True)
    print(f"BigQuery table OK: {BQ_TABLE}")

    rows = [
        {
            "id": "test_001",
            "message": "BigQuery permission test from Airflow",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    ]

    errors = client.insert_rows_json(BQ_TABLE, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    print("BigQuery insert OK")

    query = f"""
    SELECT id, message, created_at
    FROM `{BQ_TABLE}`
    WHERE id = 'test_001'
    LIMIT 1
    """

    results = list(client.query(query).result())

    if not results:
        raise RuntimeError("BigQuery query failed: no result found")

    print("BigQuery query OK")
    for row in results:
        print(dict(row))

    client.delete_table(BQ_TABLE, not_found_ok=True)
    print(f"BigQuery cleanup OK: deleted {BQ_TABLE}")


def test_storage():
    print("=== Testing Cloud Storage ===")

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_TEST_PATH)

    test_data = {
        "message": "Cloud Storage permission test from Airflow",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
        local_path = f.name

    try:
        blob.upload_from_filename(local_path)
        print(f"Storage upload OK: gs://{BUCKET_NAME}/{GCS_TEST_PATH}")

        downloaded = blob.download_as_text()
        print("Storage download OK")
        print(downloaded)

        blob.delete()
        print("Storage cleanup OK")

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


with DAG(
    dag_id="test_bq_and_storage_permission",
    description="Test Airflow permissions for BigQuery and Cloud Storage",
    start_date=datetime(2026, 7, 5),
    schedule_interval=None,
    catchup=False,
    tags=["test", "bigquery", "storage"],
) as dag:

    test_bigquery_task = PythonOperator(
        task_id="test_bigquery",
        python_callable=test_bigquery,
    )

    test_storage_task = PythonOperator(
        task_id="test_storage",
        python_callable=test_storage,
    )

    test_bigquery_task >> test_storage_task