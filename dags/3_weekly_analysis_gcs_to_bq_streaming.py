import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

# 將 GCS 指定資料夾底下的 cleaned JSONL 分析結果寫回 BigQuery reviews_analysis table。
# 串流設計：逐一讀取 GCS JSONL 檔案，逐批寫入 BigQuery，避免一次把多個檔案全部塞進記憶體。
# 注意：
# 1) 會排除 raw 資料夾底下的資料。
# 2) JSONL 單筆 record 如果含 error 欄位，整筆 review_id 直接跳過，繼續處理下一筆。
# 3) 單一面向 sentiment 為 null、空字串，或無法轉換成正負向時，跳過該面向，繼續處理下一個面向。
# 4) 寫入 reviews_analysis 時，若該 review_id 已存在，會先刪除該 review_id 的既有分析資料，再新增本次解析結果。

DAG_ID = "reviews_analysis_gcs_to_bigquery_streaming"
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"
BUCKET_NAME = "my-airflow-data-bucket-2026"
TODAY = datetime.now().strftime("%Y%m%d")  # 例如：20260701
SOURCE_PREFIX = f"analysis_final/clean_batch_results/weekly_run_{TODAY}"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reviews_analysis"

# 這是「寫入 rows 數」上限。程式只會在一整筆 review 的所有面向都放進 buffer 後才 flush，
# 避免同一個 review_id 的不同面向被拆到不同批次，造成後一批 DELETE 掉前一批剛 INSERT 的資料。
WRITE_BATCH_SIZE = 500

ISSUE_TYPE_MAP = {
    "food": 1,
    "price": 2,
    "service": 3,
    "hygiene": 4,
    "queue": 5,
    "environment": 6,
    "parking": 7,
}

SENTIMENT_MAP = {
    "positive": 1,
    "negative": -1,
    "1": 1,
    "-1": -1,
    1: 1,
    -1: -1,
}


def normalize_sentiment(value: Any) -> Optional[int]:
    """
    將模型輸出的 sentiment 正規化為 BigQuery 使用的整數：
    positive / 1  -> 1
    negative / -1 -> -1

    遇到 None、空字串、null、unknown、或其他不合法值時，回傳 None，
    後續會跳過該面向，繼續處理下一筆資料。
    """
    if value is None:
        return None

    if isinstance(value, str):
        normalized_value = value.strip().lower()
        if not normalized_value:
            return None
        return SENTIMENT_MAP.get(normalized_value)

    return SENTIMENT_MAP.get(value)


def build_keyword_text(value: Any) -> Optional[str]:
    if not isinstance(value, list) or not value:
        return None

    keywords = [str(item).strip() for item in value if item is not None and str(item).strip()]
    if not keywords:
        return None

    return "、".join(keywords)[:500]


def is_target_jsonl_object(object_name: str) -> bool:
    """只保留 SOURCE_PREFIX 底下的 .jsonl，並排除 raw 資料夾。"""
    normalized_prefix = SOURCE_PREFIX.rstrip("/") + "/"

    if not object_name.startswith(normalized_prefix):
        return False

    if not object_name.endswith(".jsonl"):
        return False

    relative_path = object_name[len(normalized_prefix) :]
    path_parts = relative_path.split("/")
    if "raw" in path_parts:
        return False

    return True


def list_source_jsonl_objects(gcs_hook: GCSHook) -> List[str]:
    objects = gcs_hook.list(
        bucket_name=BUCKET_NAME,
        prefix=SOURCE_PREFIX.rstrip("/") + "/",
    )

    jsonl_objects = sorted(
        object_name
        for object_name in objects
        if is_target_jsonl_object(object_name)
    )

    logging.info(
        "Found %s target JSONL files under gs://%s/%s",
        len(jsonl_objects),
        BUCKET_NAME,
        SOURCE_PREFIX,
    )

    for object_name in jsonl_objects:
        logging.info("Target JSONL file: gs://%s/%s", BUCKET_NAME, object_name)

    return jsonl_objects


def iter_analysis_row_groups_from_jsonl(jsonl_text: str, source_object: str) -> Iterator[List[Dict[str, Any]]]:
    """
    逐行解析 JSONL。

    每次 yield 的單位是一個 review_id 對應的多個面向 rows。
    這樣寫入端可以確保同一個 review_id 不會被拆到不同批次，避免 DELETE + INSERT 時互相覆蓋。
    """
    failed_lines = 0
    skipped_missing_review_id = 0
    skipped_error = 0
    skipped_no_content = 0
    skipped_no_valid_sentiment_review = 0
    skipped_invalid_sentiment_aspect = 0
    parsed_rows = 0
    parsed_reviews = 0
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for line_number, line in enumerate(jsonl_text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            failed_lines += 1
            logging.warning("Skip invalid JSON at %s line %s", source_object, line_number)
            continue

        review_id = record.get("review_id")
        if not review_id:
            skipped_missing_review_id += 1
            logging.warning("Skip %s line %s because review_id is missing", source_object, line_number)
            continue

        # 含有 error 欄位代表上游分析失敗，整筆 review 跳過，繼續下一筆。
        if "error" in record:
            skipped_error += 1
            logging.warning(
                "Skip %s line %s (review_id=%s) because record contains an error field",
                source_object,
                line_number,
                review_id,
            )
            continue

        has_valid_aspect = any(
            isinstance(record.get(aspect), dict) for aspect in ISSUE_TYPE_MAP
        )
        if not has_valid_aspect:
            skipped_no_content += 1
            logging.warning(
                "Skip %s line %s (review_id=%s) because no aspect data found",
                source_object,
                line_number,
                review_id,
            )
            continue

        rows_for_review: List[Dict[str, Any]] = []

        for aspect, issue_type_id in ISSUE_TYPE_MAP.items():
            data = record.get(aspect)
            if not isinstance(data, dict):
                continue

            sentiment = normalize_sentiment(data.get("sentiment"))

            # sentiment 是 null、空字串，或其他不合法值時，只跳過該面向，不中斷整支程式。
            if sentiment is None:
                skipped_invalid_sentiment_aspect += 1
                logging.warning(
                    "Skip aspect=%s at %s line %s (review_id=%s) because sentiment is null/empty/invalid",
                    aspect,
                    source_object,
                    line_number,
                    review_id,
                )
                continue

            rows_for_review.append(
                {
                    "review_id": str(review_id),
                    "issue_type_id": issue_type_id,
                    "sentiment": sentiment,
                    "ai_feedback_keywords": build_keyword_text(data.get("keywords")),
                    "created_at": loaded_at,
                }
            )

        if not rows_for_review:
            skipped_no_valid_sentiment_review += 1
            logging.warning(
                "Skip %s line %s (review_id=%s) because all aspects have null/empty/invalid sentiment",
                source_object,
                line_number,
                review_id,
            )
            continue

        parsed_reviews += 1
        parsed_rows += len(rows_for_review)
        yield rows_for_review

    logging.info(
        (
            "Parsed %s rows from %s reviews in %s. "
            "skipped_invalid_json=%s, skipped_missing_review_id=%s, skipped_error=%s, "
            "skipped_no_content=%s, skipped_invalid_sentiment_aspect=%s, "
            "skipped_no_valid_sentiment_review=%s"
        ),
        parsed_rows,
        parsed_reviews,
        source_object,
        failed_lines,
        skipped_missing_review_id,
        skipped_error,
        skipped_no_content,
        skipped_invalid_sentiment_aspect,
        skipped_no_valid_sentiment_review,
    )


def get_bigquery_client() -> bigquery.Client:
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    return hook.get_client(project_id=PROJECT_ID)


def get_next_analyze_id(client: bigquery.Client) -> int:
    """查詢目前最大的 analyze_id，回傳下一個可用編號（從 1 開始）。"""
    sql = f"SELECT IFNULL(MAX(analyze_id), 0) AS max_id FROM `{TARGET_TABLE}`"
    result = list(client.query(sql).result())
    max_id = result[0]["max_id"] if result else 0
    return max_id + 1


def insert_rows_after_deleting_review_ids(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    start_id: int,
) -> int:
    """
    寫入策略：
    1) 替本批 rows 分配 analyze_id。
    2) 建立本批 source_rows 暫存表。
    3) 先刪除 TARGET_TABLE 中本批 review_id 的所有舊資料。
    4) 再 INSERT 本批新的分析資料。

    注意：
    - DELETE 條件是 review_id，不是 review_id + issue_type_id。
      這符合「只要 review_id 存在，就先刪除後新增」的需求。
    - source_rows 會對同一批內重複的 review_id + issue_type_id 去重，只保留 created_at 最新、analyze_id 最大者。
    """
    if not rows:
        return start_id

    for offset, row in enumerate(rows):
        row["analyze_id"] = start_id + offset

    next_start_id = start_id + len(rows)

    sql = f"""
    CREATE TEMP TABLE source_rows AS
    SELECT
      analyze_id,
      review_id,
      issue_type_id,
      sentiment,
      ai_feedback_keywords,
      created_at
    FROM (
      SELECT
        SAFE_CAST(JSON_VALUE(row_json, '$.analyze_id') AS INT64) AS analyze_id,
        JSON_VALUE(row_json, '$.review_id') AS review_id,
        SAFE_CAST(JSON_VALUE(row_json, '$.issue_type_id') AS INT64) AS issue_type_id,
        SAFE_CAST(JSON_VALUE(row_json, '$.sentiment') AS INT64) AS sentiment,
        JSON_VALUE(row_json, '$.ai_feedback_keywords') AS ai_feedback_keywords,
        TIMESTAMP(JSON_VALUE(row_json, '$.created_at')) AS created_at
      FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@rows_json))) AS row_json
    )
    WHERE review_id IS NOT NULL
      AND issue_type_id IS NOT NULL
      AND sentiment IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY review_id, issue_type_id
      ORDER BY created_at DESC, analyze_id DESC
    ) = 1;

    BEGIN TRANSACTION;

    DELETE FROM `{TARGET_TABLE}`
    WHERE review_id IN (
      SELECT DISTINCT review_id
      FROM source_rows
    );

    INSERT INTO `{TARGET_TABLE}` (
      analyze_id,
      review_id,
      issue_type_id,
      sentiment,
      ai_feedback_keywords,
      created_at
    )
    SELECT
      analyze_id,
      review_id,
      issue_type_id,
      sentiment,
      ai_feedback_keywords,
      created_at
    FROM source_rows;

    COMMIT TRANSACTION;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "rows_json",
                "STRING",
                json.dumps(rows, ensure_ascii=False),
            )
        ]
    )

    client.query(sql, job_config=job_config).result()
    return next_start_id


def flush_batch(
    client: bigquery.Client,
    buffer: List[Dict[str, Any]],
    next_id: int,
    batch_number: int,
) -> int:
    review_id_count = len({row["review_id"] for row in buffer})
    logging.info(
        (
            "Writing batch %s into %s. rows=%s, review_ids=%s, "
            "starting_analyze_id=%s, mode=delete_all_old_aspects_by_review_id_then_insert_current_result"
        ),
        batch_number,
        TARGET_TABLE,
        len(buffer),
        review_id_count,
        next_id,
    )
    return insert_rows_after_deleting_review_ids(client, buffer, next_id)


def download_text_from_gcs(gcs_hook: GCSHook, object_name: str) -> str:
    raw_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=object_name)
    return raw_data.decode("utf-8") if isinstance(raw_data, bytes) else raw_data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="update_reviews_analysis",
    default_args=default_args,
    description="Stream review analysis JSONL files from GCS and replace all old aspect rows in BigQuery by review_id.",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["gcs", "bigquery", "reviews"],
) as dag:

    @task
    def sync_reviews_analysis() -> None:
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        jsonl_objects = list_source_jsonl_objects(gcs_hook)

        if not jsonl_objects:
            raise AirflowFailException(
                f"No target JSONL files found under gs://{BUCKET_NAME}/{SOURCE_PREFIX}."
            )

        client = get_bigquery_client()
        next_id = get_next_analyze_id(client)

        buffer: List[Dict[str, Any]] = []
        batch_number = 1
        total_rows = 0
        total_files = 0
        total_reviews = 0

        for object_name in jsonl_objects:
            total_files += 1
            logging.info("Downloading gs://%s/%s", BUCKET_NAME, object_name)
            jsonl_text = download_text_from_gcs(gcs_hook, object_name)

            file_rows = 0
            file_reviews = 0

            for rows_for_review in iter_analysis_row_groups_from_jsonl(jsonl_text, object_name):
                buffer.extend(rows_for_review)
                file_rows += len(rows_for_review)
                total_rows += len(rows_for_review)
                file_reviews += 1
                total_reviews += 1

                # 只在完整 review 加入 buffer 後才 flush，避免同一 review_id 被拆批。
                if len(buffer) >= WRITE_BATCH_SIZE:
                    next_id = flush_batch(client, buffer, next_id, batch_number)
                    batch_number += 1
                    buffer.clear()

            logging.info(
                "Finished %s with %s parsed reviews and %s parsed rows",
                object_name,
                file_reviews,
                file_rows,
            )

        if buffer:
            next_id = flush_batch(client, buffer, next_id, batch_number)
            buffer.clear()

        if total_rows == 0:
            raise AirflowFailException("No valid review analysis rows were parsed from target JSONL files.")

        logging.info(
            "Sync completed. files=%s, reviews=%s, rows=%s, final_next_analyze_id=%s",
            total_files,
            total_reviews,
            total_rows,
            next_id,
        )

    sync_reviews_analysis()
