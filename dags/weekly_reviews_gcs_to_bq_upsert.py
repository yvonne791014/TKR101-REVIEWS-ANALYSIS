from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# 每週更新評論後執行：
# 1. 從 GCS 讀取本週 CSV
# 2. 載入 staging table
# 3. 先計算本次真正新增的 review 數量，依 restaurant_id 彙總
# 4. MERGE reviews：review_id 存在就 UPDATE，不存在就 INSERT
# 5. 很重要：依新增評論數更新 low_rating_restaurant.reviews_count
# 6. 把本次處理過的 review_id 寫入 semantic analysis queue

PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"

REVIEWS_TABLE_ID = "reviews"
STAGING_TABLE_ID = "reviews_staging_from_gcs"
QUEUE_TABLE_ID = "review_semantic_analysis_queue"
LOW_RATING_RESTAURANT_TABLE_ID = "low_rating_restaurant"
NEW_REVIEW_COUNT_TABLE_ID = "new_review_count_temp"

BUCKET_NAME = "my-airflow-data-bucket-2026"
GCP_CONN_ID = "google_cloud_default"

TODAY = datetime.now().strftime("%m%d")  # 例如：0701
GCS_PREFIX = f"crawl-weekly/vm-{TODAY}/"


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
                    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 2) 建立語意分析 queue table
        #    後續語意分析只要讀 status = 'PENDING' 的 review_id 即可。
        ensure_semantic_queue_table = BigQueryInsertJobOperator(
            task_id="ensure_semantic_queue_table",
            configuration={
                "query": {
                    "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{QUEUE_TABLE_ID}` (
                        review_id STRING NOT NULL,
                        status STRING NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL
                    )
                    CLUSTER BY review_id;
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

        # 4) 在 MERGE reviews 之前，先計算「本次真正新增」的評論數。
        #    注意：這一步一定要在 MERGE 前做，否則 MERGE 後新 review_id 已經存在於 reviews，會算不到新增筆數。
        calculate_new_review_count = BigQueryInsertJobOperator(
            task_id="calculate_new_review_count",
            configuration={
                "query": {
                    "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{NEW_REVIEW_COUNT_TABLE_ID}` AS
                    WITH dedup_staging AS (
                        SELECT
                            review_id,
                            restaurant_id
                        FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}`
                        WHERE review_id IS NOT NULL
                          AND restaurant_id IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) = 1
                    )
                    SELECT
                        STG.restaurant_id,
                        COUNT(*) AS new_review_count
                    FROM dedup_staging AS STG
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{REVIEWS_TABLE_ID}` AS R
                    ON STG.review_id = R.review_id
                    WHERE R.review_id IS NULL
                    GROUP BY STG.restaurant_id;
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 5) 用 review_id 對 reviews 做 UPSERT：
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
                        );
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 6) 根據「本次真正新增」的評論數，更新 low_rating_restaurant.reviews_count。
        #    例如 restaurant_id = AAA 新增 2 則，reviews_count 就 +2。
        update_low_rating_restaurant_reviews_count = BigQueryInsertJobOperator(
            task_id="update_low_rating_restaurant_reviews_count",
            configuration={
                "query": {
                    "query": f"""
                    UPDATE `{PROJECT_ID}.{DATASET_ID}.{LOW_RATING_RESTAURANT_TABLE_ID}` AS T
                    SET reviews_count = COALESCE(T.reviews_count, 0) + S.new_review_count
                    FROM `{PROJECT_ID}.{DATASET_ID}.{NEW_REVIEW_COUNT_TABLE_ID}` AS S
                    WHERE T.restaurant_id = S.restaurant_id;
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # 7) 把本次處理過的 review_id 記到 queue table，提供後續語意分析使用。
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

        # 8) 清除暫存的新評論統計表。
        #    若你想保留方便 debug，可以先把這個 task 拿掉。
        drop_new_review_count_temp = BigQueryInsertJobOperator(
            task_id="drop_new_review_count_temp",
            configuration={
                "query": {
                    "query": f"""
                    DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.{NEW_REVIEW_COUNT_TABLE_ID}`;
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        [ensure_reviews_updated_at, ensure_semantic_queue_table] >> load_gcs_to_staging
        load_gcs_to_staging >> calculate_new_review_count >> merge_staging_to_reviews
        merge_staging_to_reviews >> update_low_rating_restaurant_reviews_count
        merge_staging_to_reviews >> insert_processed_review_ids_to_queue
        [update_low_rating_restaurant_reviews_count, insert_processed_review_ids_to_queue] >> drop_new_review_count_temp
