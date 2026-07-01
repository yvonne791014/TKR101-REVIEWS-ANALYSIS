from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# 每週更新評論後執行 insert or update reviews table，
# 並把本次處理過的 review_id 記到 queue table，提供後續語意分析使用。

PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW_COPY_20260630"
REVIEWS_TABLE_ID = "reviews"
STAGING_TABLE_ID = "reviews_staging_from_gcs"
QUEUE_TABLE_ID = "review_semantic_analysis_queue"
BUCKET_NAME = "my-airflow-data-bucket-2026"
GCS_PREFIX = "crawler-weekly-yvonne-test/"
GCP_CONN_ID = "google_cloud_default"


def get_gcs_files():
    """列出 GCS 指定資料夾底下所有 CSV 檔。"""
    try:
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        all_files = hook.list(bucket_name=BUCKET_NAME, prefix=GCS_PREFIX)
        return sorted([f for f in all_files if f.endswith(".csv")])
    except Exception:
        return []


REVIEW_SCHEMA = [
    {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "restaurant_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "review_score", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "review_content", "type": "STRING", "mode": "NULLABLE"},
    {"name": "food_score", "type": "STRING", "mode": "NULLABLE"},
    {"name": "service_score", "type": "STRING", "mode": "NULLABLE"},
    {"name": "atmosphere_score", "type": "STRING", "mode": "NULLABLE"},
]


with DAG(
    dag_id="weekly_reviews_gcs_to_bigquery_upsert",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["reviews", "gcs", "bigquery"],
) as dag:

    gcs_file_list = get_gcs_files()

    if not gcs_file_list:
        no_files_found = EmptyOperator(task_id="no_files_found_in_gcs")

    else:
        # 1) 確保 reviews 有 updated_at 欄位
        ensure_reviews_updated_at = BigQueryInsertJobOperator(
            task_id="ensure_reviews_updated_at",
            configuration={
                "query": {
                    "query": f"""
                    ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{REVIEWS_TABLE_ID}`
                    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 2) 建立語意分析 queue table
        #    後續語意分析只要讀 semantic_status = 'PENDING' 的 review_id 即可。
        ensure_semantic_queue_table = BigQueryInsertJobOperator(
            task_id="ensure_semantic_queue_table",
            configuration={
                "query": {
                    "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{QUEUE_TABLE_ID}` (
                        review_id STRING NOT NULL,
                        run_id STRING NOT NULL,
                        source_gcs_prefix STRING,
                        semantic_status STRING NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL
                    )
                    PARTITION BY DATE(created_at)
                    CLUSTER BY semantic_status, review_id
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 3) 先把這次 GCS CSV 全部載入 staging table
        #    staging 每次 WRITE_TRUNCATE，避免重跑時累積舊資料。
        load_gcs_to_staging = GCSToBigQueryOperator(
            task_id="load_gcs_to_staging",
            bucket=BUCKET_NAME,
            source_objects=gcs_file_list,
            destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}",
            source_format="CSV",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            skip_leading_rows=1,
            autodetect=False,
            schema_fields=REVIEW_SCHEMA,
            allow_quoted_newlines=True,
            ignore_unknown_values=True,
            max_bad_records=1000,
            gcp_conn_id=GCP_CONN_ID,
        )

        # 4) 用 review_id 對 reviews 做 UPSERT：
        #    - review_id 已存在：UPDATE，並更新 updated_at
        #    - review_id 不存在：INSERT，並寫入 updated_at
        #    QUALIFY 是為了避免 staging 內同一個 review_id 重複，造成 MERGE 報錯。
        merge_staging_to_reviews = BigQueryInsertJobOperator(
            task_id="merge_staging_to_reviews",
            configuration={
                "query": {
                    "query": f"""
                    MERGE `{PROJECT_ID}.{DATASET_ID}.{REVIEWS_TABLE_ID}` AS T
                    USING (
                        SELECT
                            review_id,
                            restaurant_id,
                            review_score,
                            review_content,
                            food_score,
                            service_score,
                            atmosphere_score
                        FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}`
                        WHERE review_id IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) = 1
                    ) AS S
                    ON T.review_id = S.review_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            restaurant_id = S.restaurant_id,
                            review_score = S.review_score,
                            review_content = S.review_content,
                            food_score = S.food_score,
                            service_score = S.service_score,
                            atmosphere_score = S.atmosphere_score,
                            updated_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN
                        INSERT (
                            review_id,
                            restaurant_id,
                            review_score,
                            review_content,
                            food_score,
                            service_score,
                            atmosphere_score,
                            updated_at
                        )
                        VALUES (
                            S.review_id,
                            S.restaurant_id,
                            S.review_score,
                            S.review_content,
                            S.food_score,
                            S.service_score,
                            S.atmosphere_score,
                            CURRENT_TIMESTAMP()
                        )
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 5) 把本次處理過的 review_id 記到 queue table，提供後續語意分析使用。
        #    若 queue 已存在同一個 review_id，不會重複塞入。
        insert_processed_review_ids_to_queue = BigQueryInsertJobOperator(
            task_id="insert_processed_review_ids_to_queue",
            configuration={
                "query": {
                    "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{QUEUE_TABLE_ID}` (
                        review_id,
                        status,
                        created_at,
                        updated_at
                    )
                    SELECT
                        S.review_id,
                        'PENDING',
                        CURRENT_TIMESTAMP(),
                        CURRENT_TIMESTAMP()
                    FROM (
                        SELECT DISTINCT review_id
                        FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}`
                        WHERE review_id IS NOT NULL
                    ) AS S
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM `{PROJECT_ID}.{DATASET_ID}.{QUEUE_TABLE_ID}` AS Q
                        WHERE Q.review_id = S.review_id
                    );
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        [ensure_reviews_updated_at, ensure_semantic_queue_table] >> load_gcs_to_staging
        load_gcs_to_staging >> merge_staging_to_reviews >> insert_processed_review_ids_to_queue
