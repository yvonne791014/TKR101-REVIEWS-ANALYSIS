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

PROJECT_ID = "taipei-restaurant-analysis"
BUCKET_NAME = "my-airflow-data-bucket-2026"

BQ_DATASET_ID = "REVIEW"
BQ_TABLE_ID = "reviews"

MODEL_NAME = "gemini-2.5-flash-lite"

# RUN_MODE:
# - first_full：第一次全量分析，來源直接撈 reviews，不更新 queue 狀態
# - weekly_queue：每週增量分析，來源撈 queue 裡 PENDING，Batch 建立成功後更新為 PROCESSING
RUN_MODE = os.getenv("RUN_MODE", "weekly_queue")

TODAY = datetime.now().strftime("%Y%m%d")

if RUN_MODE == "first_full":
    RUN_ID = f"full_run_{TODAY}"
else:
    RUN_ID = f"weekly_run_{TODAY}"


BATCH_SIZE = 5000
MAX_CREATE_PER_DAG_RUN = 10
POLL_SECONDS = 300

GCS_BASE = "analysis_final"
GCS_REQUEST_PREFIX = f"{GCS_BASE}/batch_requests/{RUN_ID}"
GCS_JOB_PREFIX = f"{GCS_BASE}/batch_jobs/{RUN_ID}"
GCS_CLEAN_PREFIX = f"{GCS_BASE}/clean_batch_results/{RUN_ID}"
GCS_ERROR_PREFIX = f"{GCS_BASE}/batch_errors/{RUN_ID}"
GCS_STATUS_BLOB = f"{GCS_BASE}/status.json"

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




def write_pipeline_status(
    bucket,
    status: str,
    message: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """
    寫入 DAG2 與 DAG3 共用的 GCS 狀態檔。

    status:
    - RUNNING：Gemini Batch 尚未全部完成，DAG3 應直接結束。
    - DONE：clean JSONL 已全部準備完成，DAG3 可以開始匯入 BigQuery。
    - IMPORTED：DAG3 已完成匯入，避免每 10 分鐘重複寫入同一批結果。
    """
    payload: dict[str, Any] = {
        "status": status,
        "run_id": RUN_ID,
        "run_mode": RUN_MODE,
        "updated_at": datetime.utcnow().isoformat(),
        "message": message,
        "bucket": BUCKET_NAME,
        "request_prefix": GCS_REQUEST_PREFIX,
        "job_prefix": GCS_JOB_PREFIX,
        "clean_prefix": GCS_CLEAN_PREFIX,
        "clean_gcs_uri": f"gs://{BUCKET_NAME}/{GCS_CLEAN_PREFIX}/",
    }

    if extra:
        payload.update(extra)

    upload_json_to_gcs(bucket, GCS_STATUS_BLOB, payload)
    print(f"🟦 status.json updated: status={status}, run_id={RUN_ID}")


def list_gcs_json(bucket, prefix: str) -> list[str]:
    return sorted(
        blob.name
        for blob in bucket.list_blobs(prefix=prefix)
        if blob.name.endswith(".json")
    )


def list_unfinished_job_meta_blobs(bucket) -> list[str]:
    """
    掃描所有 batch_jobs 底下尚未完成的 job metadata。

    原因：
    RUN_ID 會依執行日期變動，例如 weekly_run_20260701、weekly_run_20260708。
    如果某次 Batch 已送出但還沒完成，下一次 DAG 執行時本次 manifest 可能是空的，
    但前一次 RUN_ID 底下仍有需要監控與下載結果的 job checkpoint。
    """
    all_job_meta_blobs = list_gcs_json(bucket, f"{GCS_BASE}/batch_jobs/")
    unfinished: list[str] = []

    for blob_name in all_job_meta_blobs:
        if blob_name.endswith("/manifest.json"):
            continue

        meta = download_json_from_gcs(bucket, blob_name)
        if not meta:
            continue

        if not meta.get("job_name"):
            continue

        clean_gcs_uri = meta.get("clean_gcs_uri")
        if clean_gcs_uri:
            clean_blob = clean_gcs_uri.replace(f"gs://{BUCKET_NAME}/", "")
        else:
            run_id = meta.get("run_id")
            batch_index = meta.get("batch_index")
            if not run_id or not batch_index:
                continue
            clean_blob = (
                f"{GCS_BASE}/clean_batch_results/{run_id}/"
                f"batch_{int(batch_index):04d}_clean.jsonl"
            )

        if blob_exists(bucket, clean_blob):
            continue

        state = str(meta.get("state", ""))
        if is_failed_state(state):
            continue

        unfinished.append(blob_name)

    return sorted(unfinished)




def list_current_run_job_meta_blobs(bucket) -> list[str]:
    """只列出本次 RUN_ID 底下已建立的 Gemini Batch job metadata。"""
    return sorted(
        blob_name
        for blob_name in list_gcs_json(bucket, GCS_JOB_PREFIX)
        if blob_name.endswith(".json") and not blob_name.endswith("/manifest.json")
    )


def list_current_run_clean_jsonl_blobs(bucket) -> list[str]:
    """只列出本次 RUN_ID 底下已整理完成的 clean JSONL，排除 raw 資料夾。"""
    normalized_prefix = GCS_CLEAN_PREFIX.rstrip("/") + "/"
    clean_blobs: list[str] = []

    for blob in bucket.list_blobs(prefix=normalized_prefix):
        object_name = blob.name

        if not object_name.endswith(".jsonl"):
            continue

        relative_path = object_name[len(normalized_prefix):]
        path_parts = relative_path.split("/")
        if "raw" in path_parts:
            continue

        clean_blobs.append(object_name)

    return sorted(clean_blobs)




def repair_manifest_when_clean_exists(bucket) -> None:
    """
    修復舊版 DAG2 曾把 manifest.json 覆蓋成 total_batches=0 的情況。

    若本次 RUN_ID 的 clean JSONL 已存在，但 manifest.total_batches 仍為 0，
    代表這不是「沒有資料」，而是 manifest 被後續重跑誤覆蓋。
    這裡會用 clean JSONL 數量回填 total_batches，讓後續 status 判斷可接續往下跑 DAG3。
    """
    manifest_blob = f"{GCS_JOB_PREFIX}/manifest.json"
    manifest = download_json_from_gcs(bucket, manifest_blob) or {}
    manifest_total_batches = int(manifest.get("total_batches", 0) or 0)
    clean_blobs = list_current_run_clean_jsonl_blobs(bucket)

    if manifest_total_batches == 0 and clean_blobs:
        repaired_manifest = {
            **manifest,
            "run_id": RUN_ID,
            "run_mode": RUN_MODE,
            "batch_size": manifest.get("batch_size", BATCH_SIZE),
            "total_batches": len(clean_blobs),
            "items": manifest.get("items", []),
            "repaired_from_clean_jsonl": True,
            "repaired_at": datetime.utcnow().isoformat(),
            "message": "manifest 曾被覆蓋為 total_batches=0，已依本次 RUN_ID 的 clean JSONL 數量修復。",
            "clean_jsonl_blobs": clean_blobs,
        }
        upload_json_to_gcs(bucket, manifest_blob, repaired_manifest)
        print(
            "🛠️ manifest.json repaired from clean JSONL: "
            f"total_batches={len(clean_blobs)}"
        )


def count_current_run_progress(bucket) -> dict[str, int]:
    """
    統計本次 RUN_ID 的進度。

    DAG3 是否可以啟動，只看「本日/本次 RUN_ID」：
    - submitted_jobs == total_batches
    - cleaned_batches == total_batches
    - pending_jobs == 0
    - failed_jobs == 0
    """
    repair_manifest_when_clean_exists(bucket)

    manifest = download_json_from_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json") or {}
    manifest_total_batches = int(manifest.get("total_batches", 0) or 0)
    total_count = int(manifest.get("total_count", 0) or 0)

    # 防呆：Master DAG 每 10 分鐘重跑 DAG2 時，可能因 queue 已經沒有 PENDING，
    # 導致 manifest 被誤寫成 total_batches=0。
    # 若本日 clean JSONL 已存在，仍應視為本次 RUN_ID 已經有完成結果，避免 status 卡在 RUNNING。
    clean_jsonl_blobs = list_current_run_clean_jsonl_blobs(bucket)
    inferred_clean_batches = len(clean_jsonl_blobs)
    total_batches = max(manifest_total_batches, inferred_clean_batches)

    submitted_jobs = 0
    cleaned_batches = 0
    pending_jobs = 0
    failed_jobs = 0
    success_jobs = 0

    for i in range(1, total_batches + 1):
        job_meta_blob = f"{GCS_JOB_PREFIX}/batch_{i:04d}.json"
        clean_blob = f"{GCS_CLEAN_PREFIX}/batch_{i:04d}_clean.jsonl"

        if blob_exists(bucket, job_meta_blob):
            submitted_jobs += 1
            meta = download_json_from_gcs(bucket, job_meta_blob) or {}
            state = str(meta.get("state", ""))
            if is_failed_state(state):
                failed_jobs += 1
            elif blob_exists(bucket, clean_blob):
                success_jobs += 1
            else:
                pending_jobs += 1

        if blob_exists(bucket, clean_blob):
            cleaned_batches += 1

    # 如果 manifest 被覆蓋為 0，job metadata 也無法完整反推，
    # 但 clean JSONL 已存在，DAG3 已具備可匯入的輸入資料。
    if manifest_total_batches == 0 and inferred_clean_batches > 0:
        submitted_jobs = inferred_clean_batches
        cleaned_batches = inferred_clean_batches
        success_jobs = inferred_clean_batches
        pending_jobs = 0
        failed_jobs = 0
        total_batches = inferred_clean_batches

    remaining_to_submit = max(total_batches - submitted_jobs, 0)
    remaining_to_clean = max(total_batches - cleaned_batches, 0)

    return {
        "total_count": total_count,
        "total_batches": total_batches,
        "submitted_jobs": submitted_jobs,
        "cleaned_batches": cleaned_batches,
        "success_jobs": success_jobs,
        "pending_jobs": pending_jobs,
        "failed_jobs": failed_jobs,
        "remaining_to_submit": remaining_to_submit,
        "remaining_to_clean": remaining_to_clean,
    }


def update_current_run_status_from_progress(bucket, message_prefix: str = "") -> dict[str, int]:
    """
    依本次 RUN_ID 的 batch 進度更新 status.json。
    只有當本次所有 batch 都已提交、都已取回 clean JSONL，且 pending=0、failed=0，才寫 DONE。
    """
    progress = count_current_run_progress(bucket)

    total_batches = progress["total_batches"]
    # 只用「本次 RUN_ID」自己的進度判斷是否可匯入。
    # remaining_to_submit / remaining_to_clean 是衍生值，
    # submitted_jobs == total_batches 與 cleaned_batches == total_batches 已經等價代表剩餘為 0。
    ready_to_import = (
        progress["cleaned_batches"] > 0
        and progress["cleaned_batches"] == total_batches
        and progress["pending_jobs"] == 0
        and progress["failed_jobs"] == 0
    )

    if ready_to_import:
        write_pipeline_status(
            bucket,
            status="DONE",
            message=(message_prefix + "本次 RUN_ID 的所有 Gemini Batch 都已完成並取回 clean JSONL，DAG3 可以匯入 BigQuery。"),
            extra={**progress, "ready_to_import": True},
        )
    else:
        write_pipeline_status(
            bucket,
            status="RUNNING",
            message=(message_prefix + "本次 RUN_ID 尚未全部取回 clean JSONL，DAG3 應略過本次匯入。"),
            extra={**progress, "ready_to_import": False},
        )

    return progress



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



def update_queue_status(review_ids: list[str], status: str) -> None:
    """
    僅 weekly_queue 模式會更新 review_semantic_analysis_queue。
    first_full 第一次全量分析不會動 queue。
    """
    if RUN_MODE != "weekly_queue":
        print(f"ℹ️ RUN_MODE={RUN_MODE}，不更新 queue 狀態：{status}")
        return

    review_ids = [str(review_id) for review_id in review_ids if review_id]
    if not review_ids:
        print(f"ℹ️ 沒有 review_id 需要更新為 {status}")
        return

    from google.cloud import bigquery

    bq_client = bigquery.Client(project=PROJECT_ID)

    sql = f"""
        UPDATE `{PROJECT_ID}.{BQ_DATASET_ID}.review_semantic_analysis_queue`
        SET status = @status,
            updated_at = CURRENT_TIMESTAMP()
        WHERE review_id IN UNNEST(@review_ids)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ArrayQueryParameter("review_ids", "STRING", review_ids),
        ]
    )

    bq_client.query(sql, job_config=job_config).result()
    print(f"✅ queue 狀態更新為 {status}: {len(review_ids)} 筆")


def build_source_sql() -> str:
    """
    依 RUN_MODE 決定語意分析來源。
    """
    if RUN_MODE == "first_full":
        return f"""
        SELECT review_id, restaurant_id, review_score, review_content
        FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
        WHERE review_content IS NOT NULL
          AND TRIM(review_content) != ''
          AND review_score <= 3
        ORDER BY review_id limit 1
        """

    return f"""
        SELECT r.review_id, r.restaurant_id, r.review_score, r.review_content
        FROM `{PROJECT_ID}.{BQ_DATASET_ID}.review_semantic_analysis_queue` q
        INNER JOIN `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}` r
          ON q.review_id = r.review_id
        WHERE q.status = 'PENDING'
        ORDER BY r.review_id
    """


def task_prepare_and_upload_requests(**context):
    from google.cloud import bigquery

    print("📋 Task 1: 從 BigQuery 撈資料、分批建立 JSONL、上傳 GCS")
    print(f"🏷️ RUN_MODE={RUN_MODE}")
    print(f"🏷️ RUN_ID={RUN_ID}")

    bq_client = bigquery.Client(project=PROJECT_ID)
    bucket = get_bucket()

    existing_manifest = download_json_from_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json")
    existing_total_batches = int((existing_manifest or {}).get("total_batches", 0) or 0)
    existing_clean_count = len(list_current_run_clean_jsonl_blobs(bucket))

    if existing_total_batches > 0 or existing_clean_count > 0:
        print(
            "ℹ️ 本次 RUN_ID 已存在 manifest 或 clean JSONL，"
            "不重新建立 request，也不覆蓋既有 manifest。"
        )
        progress = update_current_run_status_from_progress(
            bucket,
            message_prefix="prepare_and_upload_requests 偵測到本次 RUN_ID 已有既有批次結果。",
        )
        print(f"🟦 current_run_progress={progress}")
        return

    write_pipeline_status(
        bucket,
        status="RUNNING",
        message="DAG2 已開始準備 Gemini Batch request，DAG3 暫不可匯入。",
    )

    sql_query = build_source_sql()
    rows = bq_client.query(sql_query).result()

    batch_index = 1
    current_count = 0
    total_count = 0
    current_review_ids: list[str] = []
    manifest_items: list[dict[str, Any]] = []

    def open_file(index: int):
        path = LOCAL_DIR / f"gemini_batch_requests_{index:04d}.jsonl"
        return path, open(path, "w", encoding="utf-8")

    def upload_current_batch(index: int, local_path: Path, count: int, review_ids: list[str]) -> None:
        gcs_request_blob = f"{GCS_REQUEST_PREFIX}/batch_{index:04d}.jsonl"
        bucket.blob(gcs_request_blob).upload_from_filename(str(local_path))

        manifest_items.append(
            {
                "batch_index": index,
                "local_file": str(local_path),
                "request_gcs_uri": f"gs://{BUCKET_NAME}/{gcs_request_blob}",
                "request_gcs_blob": gcs_request_blob,
                "count": count,
                "review_ids": review_ids,
            }
        )

        print(f"📤 上傳 request batch {index:04d}: {count} 筆")

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
        current_review_ids.append(review_id)
        current_count += 1
        total_count += 1

        if current_count >= BATCH_SIZE:
            current_file.close()

            upload_current_batch(
                index=batch_index,
                local_path=current_path,
                count=current_count,
                review_ids=current_review_ids,
            )

            batch_index += 1
            current_count = 0
            current_review_ids = []
            current_path, current_file = open_file(batch_index)

    current_file.close()

    if current_count > 0:
        upload_current_batch(
            index=batch_index,
            local_path=current_path,
            count=current_count,
            review_ids=current_review_ids,
        )
    else:
        current_path.unlink(missing_ok=True)

    if not manifest_items:
        manifest = {
            "run_id": RUN_ID,
            "run_mode": RUN_MODE,
            "created_at": datetime.utcnow().isoformat(),
            "batch_size": BATCH_SIZE,
            "total_count": 0,
            "total_batches": 0,
            "items": [],
            "message": "沒有可分析的評論資料",
        }
        upload_json_to_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json", manifest)
        write_pipeline_status(
            bucket,
            status="DONE",
            message="本次沒有可分析的評論資料，DAG3 可安全略過匯入。",
            extra={
                "total_count": 0,
                "total_batches": 0,
                "ready_to_import": False,
            },
        )
        print("✅ 沒有可分析的評論資料，本次不建立 batch")
        return

    manifest = {
        "run_id": RUN_ID,
        "run_mode": RUN_MODE,
        "created_at": datetime.utcnow().isoformat(),
        "batch_size": BATCH_SIZE,
        "total_count": total_count,
        "total_batches": len(manifest_items),
        "items": manifest_items,
    }

    upload_json_to_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json", manifest)
    write_pipeline_status(
        bucket,
        status="RUNNING",
        message="Gemini Batch request 已建立，等待提交與模型非同步分析完成。",
        extra={
            "total_count": total_count,
            "total_batches": len(manifest_items),
            "ready_to_import": False,
        },
    )

    print(f"✅ 共 {total_count} 筆，建立 {len(manifest_items)} 個 request JSONL")


def task_submit_pending_batches(**context):
    from google.genai import errors

    print("🚀 Task 2: 提交尚未建立的 Gemini Batch，並寫入 GCS checkpoint")

    client = init_gemini_client()
    bucket = get_bucket()

    manifest = download_json_from_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json")
    if not manifest:
        write_pipeline_status(
            bucket,
            status="RUNNING",
            message="找不到本次 manifest.json，DAG3 不應匯入。",
            extra={"ready_to_import": False},
        )
        print("✅ 找不到 manifest.json，本次沒有 batch 需要提交")
        return

    if not manifest.get("items"):
        progress = update_current_run_status_from_progress(
            bucket,
            message_prefix="submit_pending_batches 偵測到 manifest 沒有 items。",
        )
        print(f"✅ 本次沒有 batch 需要提交，current_run_progress={progress}")
        return

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
                "run_mode": RUN_MODE,
                "batch_index": batch_index,
                "request_gcs_uri": item["request_gcs_uri"],
                "input_file_name": uploaded_file.name,
                "job_name": batch_job.name,
                "state": state,
                "created_at": datetime.utcnow().isoformat(),
                "clean_gcs_uri": f"gs://{BUCKET_NAME}/{clean_blob}",
                "review_ids": item.get("review_ids", []),
            }

            upload_json_to_gcs(bucket, job_meta_blob, job_meta)

            if RUN_MODE == "weekly_queue":
                update_queue_status(
                    review_ids=item.get("review_ids", []),
                    status="PROCESSING",
                )

            print(f"✅ Batch 建立成功：batch_{batch_index:04d}, {batch_job.name}")

            created_count += 1

        except errors.ClientError as e:
            error_text = str(e)

            if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
                print("⚠️ 遇到 429 / RESOURCE_EXHAUSTED，停止提交新 Batch")
                print("✅ 已建立的 Batch 會繼續在 Google 背景執行")
                break

            raise

    progress = update_current_run_status_from_progress(
        bucket,
        message_prefix=f"submit_pending_batches 本輪新建立 Batch 數量：{created_count}。",
    )
    print(f"✅ 本輪新建立 Batch 數量：{created_count}, current_run_progress={progress}")


def task_monitor_download_clean_upload(**context):
    print("🔎 Task 3: 監控已建立 Batch，完成者下載、清理、上傳 GCS")

    client = init_gemini_client()
    bucket = get_bucket()

    # 不要只看本次 RUN_ID 的 manifest。
    # 因為 RUN_ID 會依日期變動，上一輪已送出但尚未完成的 batch
    # 可能在前一個 RUN_ID 的 GCS_JOB_PREFIX 底下。
    job_meta_blobs = list_unfinished_job_meta_blobs(bucket)
    if not job_meta_blobs:
        print("目前沒有任何尚未完成的 batch checkpoint")
        # 即使沒有未完成 checkpoint，也要重新計算本次 RUN_ID 的進度。
        # 若本次所有 clean JSONL 都已存在，這裡就會把 status.json 更新為 DONE。
        progress = update_current_run_status_from_progress(
            bucket,
            message_prefix="monitor_download_clean_upload 未發現未完成 checkpoint。",
        )
        print(f"🟦 current_run_progress={progress}")
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
        review_ids = meta.get("review_ids", [])

        meta_run_id = meta.get("run_id", RUN_ID)
        clean_prefix = f"{GCS_BASE}/clean_batch_results/{meta_run_id}"
        error_prefix = f"{GCS_BASE}/batch_errors/{meta_run_id}"

        clean_blob = f"{clean_prefix}/batch_{batch_index:04d}_clean.jsonl"
        raw_blob = f"{clean_prefix}/raw/batch_{batch_index:04d}_raw.jsonl"
        error_blob = f"{error_prefix}/batch_{batch_index:04d}_error.json"

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
            meta["failed_at"] = datetime.utcnow().isoformat()
            upload_json_to_gcs(bucket, error_blob, meta)
            upload_json_to_gcs(bucket, job_meta_blob, meta)

            if RUN_MODE == "weekly_queue":
                update_queue_status(review_ids=review_ids, status="FAILED")

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

        if RUN_MODE == "weekly_queue":
            update_queue_status(review_ids=review_ids, status="SUCCESS")

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

    # 關鍵同步點：DAG3 只能在本次 RUN_ID 的所有 batch 都已取回 clean JSONL 後才執行。
    # 這裡會用本次 RUN_ID 的 submitted / cleaned / pending / failed 重新計算，
    # 避免只因為某次 monitor 沒有掃到 job，就誤判為完成。
    progress = update_current_run_status_from_progress(
        bucket,
        message_prefix=(
            f"monitor_download_clean_upload 統計："
            f"completed={completed_count}, pending={pending_count}, failed={failed_count}。"
        ),
    )
    print(f"🟦 current_run_progress={progress}")


def task_report_status(**context):
    bucket = get_bucket()

    manifest = download_json_from_gcs(bucket, f"{GCS_JOB_PREFIX}/manifest.json")
    if not manifest:
        print("找不到 manifest.json")
        write_pipeline_status(
            bucket,
            status="RUNNING",
            message="找不到本次 manifest.json，DAG3 不應匯入。",
            extra={"ready_to_import": False},
        )
        return

    progress = update_current_run_status_from_progress(bucket)

    print("📊 CURRENT RUN STATUS")
    print(f"RUN_MODE={manifest.get('run_mode', RUN_MODE)}")
    print(f"RUN_ID={RUN_ID}")
    print(f"total_reviews={progress['total_count']}")
    print(f"total_batches={progress['total_batches']}")
    print(f"submitted_jobs={progress['submitted_jobs']}")
    print(f"cleaned_batches={progress['cleaned_batches']}")
    print(f"pending_jobs={progress['pending_jobs']}")
    print(f"failed_jobs={progress['failed_jobs']}")
    print(f"remaining_to_submit={progress['remaining_to_submit']}")
    print(f"remaining_to_clean={progress['remaining_to_clean']}")
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
    dag_id="2_gemini_sentiment_batch_analysis_resumable",
    default_args=default_args,
    description="Resumable Gemini Batch 餐廳評論多面向情感分析",
    schedule_interval=None,
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