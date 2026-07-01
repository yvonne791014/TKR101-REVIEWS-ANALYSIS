import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

# 將 GCS 指定資料夾底下的 cleaned JSONL 分析結果寫回 BigQuery reviews_analysis table。
# 串流設計：逐一讀取 GCS JSONL 檔案，逐批 MERGE 到 BigQuery，避免一次把 28 個檔案全部塞進記憶體。
# 注意：會排除 raw 資料夾底下的資料。

DAG_ID = "reviews_analysis_gcs_to_bigquery_streaming"
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"
BUCKET_NAME = "my-airflow-data-bucket-2026"
TODAY = datetime.now().strftime("%Y%m%d")  # 例如：20260701
SOURCE_PREFIX = f"analysis_final/clean_batch_results/weekly_run_{TODAY}"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reviews_analysis"
MERGE_BATCH_SIZE = 500

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
    return SENTIMENT_MAP.get(value)


def build_keyword_text(value: Any) -> Optional[str]:
    if not isinstance(value, list) or not value:
        return None

    keywords = [str(item).strip() for item in value if item is not None and str(item).strip()]
    if not keywords:
        return None

    return "\u3001".join(keywords)[:500]


def chunk_rows(rows: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


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


def iter_analysis_rows_from_jsonl(jsonl_text: str, source_object: str) -> Iterator[Dict[str, Any]]:
    failed_lines = 0
    skipped_no_content = 0
    skipped_error = 0
    parsed_rows = 0
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
            logging.warning("Skip %s line %s because review_id is missing", source_object, line_number)
            continue

        # 規則 2：含有 error 欄位的記錄直接跳過（代表上游分析失敗）
        if "error" in record:
            skipped_error += 1
            logging.warning(
                "Skip %s line %s (review_id=%s) because record contains an error field",
                source_object,
                line_number,
                review_id,
            )
            continue

        # 規則 1：除了 review_id 外，沒有任何有效面向資料的記錄直接跳過
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

        for aspect, issue_type_id in ISSUE_TYPE_MAP.items():
            data = record.get(aspect)
            if not isinstance(data, dict):
                continue

            sentiment = normalize_sentiment(data.get("sentiment"))
            if sentiment is None:
                continue

            parsed_rows += 1
            yield {
                "review_id": str(review_id),
                "issue_type_id": issue_type_id,
                "sentiment": sentiment,
                "ai_feedback_keywords": build_keyword_text(data.get("keywords")),
                "created_at": loaded_at,
            }

    logging.info(
        "Parsed %s rows from %s. skipped_invalid_json=%s, skipped_error=%s, skipped_no_content=%s",
        parsed_rows,
        source_object,
        failed_lines,
        skipped_error,
        skipped_no_content,
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


def merge_rows_to_target(client: bigquery.Client, rows: List[Dict[str, Any]], start_id: int) -> int:
    """
    把 rows 寫入 target table，並依序分配連續的 analyze_id。
    回傳下一批可用的起始編號。
    """
    if not rows:
        return start_id

    for offset, row in enumerate(rows):
        row["analyze_id"] = start_id + offset
    next_start_id = start_id + len(rows)

    sql = f"""
    MERGE `{TARGET_TABLE}` AS target
    USING (
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
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY review_id, issue_type_id
        ORDER BY created_at DESC
      ) = 1
    ) AS source
    ON target.review_id = source.review_id
       AND target.issue_type_id = source.issue_type_id
    WHEN MATCHED THEN
      UPDATE SET
        sentiment = source.sentiment,
        ai_feedback_keywords = source.ai_feedback_keywords,
        created_at = COALESCE(target.created_at, source.created_at)
    WHEN NOT MATCHED THEN
      INSERT (analyze_id, review_id, issue_type_id, sentiment, ai_feedback_keywords, created_at)
      VALUES (
        source.analyze_id,
        source.review_id,
        source.issue_type_id,
        source.sentiment,
        source.ai_feedback_keywords,
        source.created_at
      )
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
    logging.info(
        "Merging batch %s with %s rows into %s (starting analyze_id=%s)",
        batch_number,
        len(buffer),
        TARGET_TABLE,
        next_id,
    )
    return merge_rows_to_target(client, buffer, next_id)


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
    dag_id=DAG_ID,
    default_args=default_args,
    description="Stream review analysis JSONL files from GCS and sync cleaned rows to BigQuery.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
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

        for object_name in jsonl_objects:
            total_files += 1
            logging.info("Downloading gs://%s/%s", BUCKET_NAME, object_name)
            jsonl_text = download_text_from_gcs(gcs_hook, object_name)

            file_rows = 0
            for row in iter_analysis_rows_from_jsonl(jsonl_text, object_name):
                buffer.append(row)
                file_rows += 1
                total_rows += 1

                if len(buffer) >= MERGE_BATCH_SIZE:
                    next_id = flush_batch(client, buffer, next_id, batch_number)
                    batch_number += 1
                    buffer.clear()

            logging.info("Finished %s with %s parsed rows", object_name, file_rows)

        if buffer:
            next_id = flush_batch(client, buffer, next_id, batch_number)
            buffer.clear()

        if total_rows == 0:
            raise AirflowFailException("No valid review analysis rows were parsed from target JSONL files.")

        logging.info(
            "Sync completed. files=%s, rows=%s, final_next_analyze_id=%s",
            total_files,
            total_rows,
            next_id,
        )

    sync_reviews_analysis()
