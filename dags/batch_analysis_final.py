from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

# 第一次抓取全部13多萬筆評論執行語意分析 ORDER BY review_id

PROJECT_ID = "taipei-restaurant-analysis"
BUCKET_NAME = "my-airflow-data-bucket-2026"

BQ_DATASET_ID = "REVIEW"
BQ_TABLE_ID = "reviews"

MODEL_NAME = "gemini-2.5-flash-lite"

RUN_ID = "full_130k_run_20260629_final_v1"

BATCH_SIZE = 5000
MAX_CREATE_PER_DAG_RUN = 10
POLL_SECONDS = 300

GCS_BASE = "analysis_final"
GCS_REQUEST_PREFIX = f"{GCS_BASE}/batch_requests/{RUN_ID}"
GCS_JOB_PREFIX = f"{GCS_BASE}/batch_jobs/{RUN_ID}"
GCS_CLEAN_PREFIX = f"{GCS_BASE}/clean_batch_results/{RUN_ID}"
GCS_ERROR_PREFIX = f"{GCS_BASE}/batch_errors/{RUN_ID}"

LOCAL_DIR = Path("/tmp/gemini_batch") / RUN_ID
LOCAL_DIR.mkdir(parents=True, exist_ok=True)


def init_gemini_client():
    from google import genai

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("找不到 GEMINI_API_KEY")

    return genai.Client(api_key=api_key)


def get_bucket():
    from google.cloud import storage

    storage_client = storage.Client(project=PROJECT_ID)
    return storage_client.bucket(BUCKET_NAME)


def blob_exists(bucket, blob_name: str) -> bool:
    return bucket.blob(blob_name).exists()


def upload_json_to_gcs(bucket, blob_name: str, data: dict[str, Any]) -> None:
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    print(f"📤 JSON uploaded: gs://{BUCKET_NAME}/{blob_name}")


def download_json_from_gcs(bucket, blob_name: str) -> dict[str, Any] | None:
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text(encoding="utf-8"))


def list_gcs_json(bucket, prefix: str) -> list[str]:
    return sorted(
        blob.name
        for blob in bucket.list_blobs(prefix=prefix)
        if blob.name.endswith(".json")
    )


def build_prompt(review_content: str, review_id: str) -> str:
    return f"""你是餐廳評論分析器。只回傳合法 JSON，不要 markdown。

只輸出評論有提到的面向：
food, price, service, hygiene, queue, environment, parking

sentiment：
1=正面
-1=負面

keywords 保留評論原文詞句。

格式：
{{"review_id":"{review_id}","food":{{"sentiment":1,"keywords":["好吃"]}}}}

review_id:
{review_id}

評論：
{review_content}
"""


def normalize_job_state(state) -> str:
    if hasattr(state, "name"):
        return state.name
    return str(state)


def is_success_state(state: str) -> bool:
    return state in {"SUCCEEDED", "JOB_STATE_SUCCEEDED"}


def is_running_or_pending_state(state: str) -> bool:
    return state in {
        "PENDING",
        "RUNNING",
        "JOB_STATE_PENDING",
        "JOB_STATE_RUNNING",
    }


def is_failed_state(state: str) -> bool:
    return state in {
        "FAILED",
        "CANCELLED",
        "EXPIRED",
        "JOB_STATE_FAILED",
        "JOB_STATE_CANCELLED",
        "JOB_STATE_EXPIRED",
    }


def get_output_file_name(batch_job) -> str:
    if hasattr(batch_job, "dest") and batch_job.dest:
        if getattr(batch_job.dest, "file_name", None):
            return batch_job.dest.file_name

    if hasattr(batch_job, "destination") and batch_job.destination:
        if getattr(batch_job.destination, "file_name", None):
            return batch_job.destination.file_name

    if getattr(batch_job, "output_file", None):
        return batch_job.output_file

    raise ValueError(f"找不到 Batch output file: {batch_job}")


def extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()

    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def extract_json_from_response_line(line: str) -> dict[str, Any]:
    try:
        data = json.loads(line)
    except Exception as e:
        return {
            "review_id": None,
            "error": "output_line_json_parse_failed",
            "exception": str(e),
            "raw_line": line[:1000],
        }

    key = data.get("key")

    if "error" in data:
        return {
            "review_id": key,
            "error": data["error"],
        }

    response = data.get("response", {})
    candidates = response.get("candidates", [])

    if not candidates:
        return {
            "review_id": key,
            "error": "no_candidates",
            "raw": data,
        }

    parts = candidates[0].get("content", {}).get("parts", [])
    if not parts:
        return {
            "review_id": key,
            "error": "no_parts",
            "raw": data,
        }

    text = parts[0].get("text", "").strip()

    try:
        parsed = extract_json_object(text)
        if "review_id" not in parsed:
            parsed["review_id"] = key
        return parsed
    except Exception as e:
        return {
            "review_id": key,
            "error": "json_parse_failed",
            "exception": str(e),
            "raw_text": text[:2000],
        }


def task_prepare_and_upload_requests(**context):
    from google.cloud import bigquery

    print("📋 Task 1: 從 BigQuery 撈資料、分批建立 JSONL、上傳 GCS")

    bq_client = bigquery.Client(project=PROJECT_ID)
    bucket = get_bucket()

    sql_query = f"""
        SELECT review_id, restaurant_id, review_score, review_content
        FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
        WHERE review_content IS NOT NULL
        AND TRIM(review_content) != ''
        AND review_score <= 3
        ORDER BY review_id
    """

    rows = bq_client.query(sql_query).result()

    batch_index = 1
    current_count = 0
    total_count = 0
    manifest_items: list[dict[str, Any]] = []

    def open_file(index: int):
        path = LOCAL_DIR / f"gemini_batch_requests_{index:04d}.jsonl"
        return path, open(path, "w", encoding="utf-8")

    current_path, current_file = open_file(batch_index)

    for row in rows:
        review_id = str(row.review_id)
        review_content = str(row.review_content).strip()

        request_item = {
            "key": review_id,
            "request": {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {
                                "text": build_prompt(review_content, review_id)
                            }
                        ],
                    }
                ],
                "generationConfig": {
                    "temperature": 0.0,
                    "responseMimeType": "application/json",
                },
            },
        }

        current_file.write(json.dumps(request_item, ensure_ascii=False) + "\n")
        current_count += 1
        total_count += 1

        if current_count >= BATCH_SIZE:
            current_file.close()

            gcs_request_blob = f"{GCS_REQUEST_PREFIX}/batch_{batch_index:04d}.jsonl"
            bucket.blob(gcs_request_blob).upload_from_filename(str(current_path))

            manifest_items.append(
                {
                    "batch_index": batch_index,
                    "local_file": str(current_path),
                    "request_gcs_uri": f"gs://{BUCKET_NAME}/{gcs_request_blob}",
                    "request_gcs_blob": gcs_request_blob,
                    "count": current_count,
                }
            )

            print(f"📤 上傳 request batch {batch_index:04d}: {current_count} 筆")

            batch_index += 1
            current_count = 0
            current_path, current_file = open_file(batch_index)

    current_file.close()

    if current_count > 0:
        gcs_request_blob = f"{GCS_REQUEST_PREFIX}/batch_{batch_index:04d}.jsonl"
        bucket.blob(gcs_request_blob).upload_from_filename(str(current_path))

        manifest_items.append(
            {
                "batch_index": batch_index,
                "local_file": str(current_path),
                "request_gcs_uri": f"gs://{BUCKET_NAME}/{gcs_request_blob}",
                "request_gcs_blob": gcs_request_blob,
                "count": current_count,
            }
        )

        print(f"📤 上傳 request batch {batch_index:04d}: {current_count} 筆")
    else:
        current_path.unlink(missing_ok=True)

    if not manifest_items:
        raise ValueError("沒有可分析的評論資料")

    manifest = {
        "run_id": RUN_ID,
        "created_at": datetime.utcnow().isoformat(),
        "batch_size": BATCH_SIZE,
        "total_count": total_count,
        "total_batches": len(manifest_items),
        "items": manifest_items,
    }

    upload_json_to_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json", manifest)

    print(f"✅ 共 {total_count} 筆，建立 {len(manifest_items)} 個 request JSONL")


def task_submit_pending_batches(**context):
    from google.genai import errors

    print("🚀 Task 2: 提交尚未建立的 Gemini Batch，並寫入 GCS checkpoint")

    client = init_gemini_client()
    bucket = get_bucket()

    manifest = download_json_from_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json")
    if not manifest:
        raise ValueError("找不到 manifest.json，請先執行 Task 1")

    created_count = 0

    for item in manifest["items"]:
        batch_index = item["batch_index"]

        job_meta_blob = f"{GCS_JOB_PREFIX}/batch_{batch_index:04d}.json"
        clean_blob = f"{GCS_CLEAN_PREFIX}/batch_{batch_index:04d}_clean.jsonl"

        if blob_exists(bucket, clean_blob):
            print(f"⏭️ batch_{batch_index:04d} clean result 已存在，跳過")
            continue

        existing_meta = download_json_from_gcs(bucket, job_meta_blob)
        if existing_meta and existing_meta.get("job_name"):
            print(
                f"⏭️ batch_{batch_index:04d} 已有 job checkpoint："
                f"{existing_meta['job_name']}"
            )
            continue

        if created_count >= MAX_CREATE_PER_DAG_RUN:
            print(f"⏸️ 本輪最多建立 {MAX_CREATE_PER_DAG_RUN} 個 Batch，停止提交")
            break

        local_file = LOCAL_DIR / f"submit_batch_{batch_index:04d}.jsonl"
        bucket.blob(item["request_gcs_blob"]).download_to_filename(str(local_file))

        print(f"📄 上傳 Gemini File API: batch_{batch_index:04d}")
        uploaded_file = client.files.upload(
            file=str(local_file),
            config={"mime_type": "application/jsonl"},
        )

        while uploaded_file.state.name != "ACTIVE":
            print(f"等待檔案 ACTIVE，目前：{uploaded_file.state.name}")
            time.sleep(5)
            uploaded_file = client.files.get(name=uploaded_file.name)

        try:
            batch_job = client.batches.create(
                model=MODEL_NAME,
                src=uploaded_file.name,
                config={
                    "display_name": f"restaurant-review-batch-{RUN_ID}-{batch_index:04d}"
                },
            )

            state = normalize_job_state(batch_job.state)

            job_meta = {
                "run_id": RUN_ID,
                "batch_index": batch_index,
                "request_gcs_uri": item["request_gcs_uri"],
                "input_file_name": uploaded_file.name,
                "job_name": batch_job.name,
                "state": state,
                "created_at": datetime.utcnow().isoformat(),
                "clean_gcs_uri": f"gs://{BUCKET_NAME}/{clean_blob}",
            }

            upload_json_to_gcs(bucket, job_meta_blob, job_meta)

            print(f"✅ Batch 建立成功：batch_{batch_index:04d}, {batch_job.name}")

            created_count += 1

        except errors.ClientError as e:
            error_text = str(e)

            if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
                print("⚠️ 遇到 429 / RESOURCE_EXHAUSTED，停止提交新 Batch")
                print("✅ 已建立的 Batch 會繼續在 Google 背景執行")
                break

            raise

    print(f"✅ 本輪新建立 Batch 數量：{created_count}")


def task_monitor_download_clean_upload(**context):
    print("🔎 Task 3: 監控已建立 Batch，完成者下載、清理、上傳 GCS")

    client = init_gemini_client()
    bucket = get_bucket()

    job_meta_blobs = list_gcs_json(bucket, f"{GCS_JOB_PREFIX}/batch_")
    if not job_meta_blobs:
        print("目前沒有任何 batch checkpoint")
        return

    completed_count = 0
    pending_count = 0
    failed_count = 0

    for job_meta_blob in job_meta_blobs:
        meta = download_json_from_gcs(bucket, job_meta_blob)
        if not meta:
            continue

        batch_index = int(meta["batch_index"])
        job_name = meta["job_name"]

        clean_blob = f"{GCS_CLEAN_PREFIX}/batch_{batch_index:04d}_clean.jsonl"
        raw_blob = f"{GCS_CLEAN_PREFIX}/raw/batch_{batch_index:04d}_raw.jsonl"
        error_blob = f"{GCS_ERROR_PREFIX}/batch_{batch_index:04d}_error.json"

        if blob_exists(bucket, clean_blob):
            print(f"⏭️ batch_{batch_index:04d} 已有 clean result，跳過")
            completed_count += 1
            continue

        job = client.batches.get(name=job_name)
        state = normalize_job_state(job.state)

        print(f"⏰ batch_{batch_index:04d} {job_name} state={state}")

        meta["state"] = state
        meta["last_checked_at"] = datetime.utcnow().isoformat()

        if is_running_or_pending_state(state):
            pending_count += 1
            upload_json_to_gcs(bucket, job_meta_blob, meta)
            continue

        if is_failed_state(state):
            failed_count += 1
            meta["failed_job"] = str(job)
            upload_json_to_gcs(bucket, error_blob, meta)
            upload_json_to_gcs(bucket, job_meta_blob, meta)
            print(f"❌ batch_{batch_index:04d} 失敗，已寫入 error metadata")
            continue

        if not is_success_state(state):
            pending_count += 1
            upload_json_to_gcs(bucket, job_meta_blob, meta)
            continue

        output_file_name = get_output_file_name(job)

        raw_path = LOCAL_DIR / f"batch_{batch_index:04d}_raw.jsonl"
        clean_path = LOCAL_DIR / f"batch_{batch_index:04d}_clean.jsonl"

        print(f"⬇️ 下載結果：batch_{batch_index:04d}, {output_file_name}")

        try:
            client.files.download(
                file=output_file_name,
                download_path=str(raw_path),
            )
        except TypeError:
            output_bytes = client.files.download(file=output_file_name)
            raw_path.write_bytes(output_bytes)

        success_count = 0
        error_count = 0

        with open(raw_path, "rb") as infile, open(
            clean_path, "w", encoding="utf-8"
        ) as outfile:
            for raw_line in infile:
                line = raw_line.decode("utf-8", errors="replace").strip()

                if not line:
                    continue

                result = extract_json_from_response_line(line)
                outfile.write(json.dumps(result, ensure_ascii=False) + "\n")

                if "error" in result:
                    error_count += 1
                else:
                    success_count += 1

        bucket.blob(raw_blob).upload_from_filename(str(raw_path))
        bucket.blob(clean_blob).upload_from_filename(str(clean_path))

        meta["state"] = state
        meta["output_file_name"] = output_file_name
        meta["raw_gcs_uri"] = f"gs://{BUCKET_NAME}/{raw_blob}"
        meta["clean_gcs_uri"] = f"gs://{BUCKET_NAME}/{clean_blob}"
        meta["success_count"] = success_count
        meta["error_count"] = error_count
        meta["finished_at"] = datetime.utcnow().isoformat()

        upload_json_to_gcs(bucket, job_meta_blob, meta)

        print(
            f"✅ batch_{batch_index:04d} 清理完成："
            f"success={success_count}, error={error_count}"
        )

        completed_count += 1

        try:
            raw_path.unlink(missing_ok=True)
            clean_path.unlink(missing_ok=True)
        except Exception as e:
            print(f"⚠️ 清理本地暫存檔失敗：{e}")

    print("====================================")
    print(f"✅ completed={completed_count}")
    print(f"⏳ pending={pending_count}")
    print(f"❌ failed={failed_count}")
    print("====================================")


def task_report_status(**context):
    bucket = get_bucket()

    manifest = download_json_from_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json")
    if not manifest:
        print("找不到 manifest.json")
        return

    total_batches = manifest["total_batches"]

    clean_count = 0
    job_count = 0

    for i in range(1, total_batches + 1):
        if blob_exists(bucket, f"{GCS_CLEAN_PREFIX}/batch_{i:04d}_clean.jsonl"):
            clean_count += 1
        if blob_exists(bucket, f"{GCS_JOB_PREFIX}/batch_{i:04d}.json"):
            job_count += 1

    print("📊 RUN STATUS")
    print(f"RUN_ID={RUN_ID}")
    print(f"total_reviews={manifest['total_count']}")
    print(f"total_batches={total_batches}")
    print(f"submitted_jobs={job_count}")
    print(f"cleaned_batches={clean_count}")
    print(f"remaining_to_submit={total_batches - job_count}")
    print(f"remaining_to_clean={total_batches - clean_count}")
    print(f"clean_result_prefix=gs://{BUCKET_NAME}/{GCS_CLEAN_PREFIX}/")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 29),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


with DAG(
    dag_id="gemini_sentiment_batch_analysis_resumable",
    default_args=default_args,
    description="Resumable Gemini Batch 餐廳評論多面向情感分析",
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gemini", "batch", "sentiment", "gcs", "resumable"],
) as dag:
    
    prepare_requests = PythonOperator(
        task_id="prepare_and_upload_requests",
        python_callable=task_prepare_and_upload_requests,
        execution_timeout=timedelta(hours=6),
    )

    submit_batches = PythonOperator(
        task_id="submit_pending_batches",
        python_callable=task_submit_pending_batches,
        execution_timeout=timedelta(hours=3),
    )

    monitor_download = PythonOperator(
        task_id="monitor_download_clean_upload",
        python_callable=task_monitor_download_clean_upload,
        execution_timeout=timedelta(hours=6),
    )

    report_status = PythonOperator(
        task_id="report_status",
        python_callable=task_report_status,
    )

    prepare_requests >> submit_batches >> monitor_download >> report_status