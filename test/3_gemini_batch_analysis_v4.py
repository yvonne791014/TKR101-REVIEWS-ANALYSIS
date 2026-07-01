import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ID = "taipei-restaurant-analysis"
BUCKET_NAME = "my-airflow-data-bucket-2026"
GCS_FOLDER = "analysis"

BQ_DATASET_ID = "REVIEW"
BQ_TABLE_ID = "reviews"

LOCAL_DIR = Path("/tmp/gemini_batch")
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 3000
POLL_SECONDS = 60
CREATE_BATCH_RETRY_SECONDS = 300

MODEL_NAME = "gemini-2.5-flash-lite"

RUN_ID = "full_130k_run"


def init_gemini_client():
    from google import genai

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("找不到 GEMINI_API_KEY")

    return genai.Client(api_key=api_key)


def build_prompt(review_content, review_id):
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


def extract_and_prepare_reviews(**context):
    from google.cloud import bigquery

    print("📋 Task 1: 從 BigQuery 撈資料並分批建立 JSONL")

    bq_client = bigquery.Client(project=PROJECT_ID)

    sql_query = f"""
        SELECT review_id, restaurant_id, review_score, review_content
        FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
        WHERE review_content IS NOT NULL
          AND TRIM(review_content) != ''
        ORDER BY review_id;
    """

    rows = bq_client.query(sql_query).result()

    request_files = []
    current_count = 0
    total_count = 0
    batch_index = 1

    def open_new_file(index):
        path = LOCAL_DIR / f"gemini_batch_requests_{index:04d}.jsonl"
        return path, open(path, "w", encoding="utf-8")

    current_path, current_file = open_new_file(batch_index)

    for row in rows:
        review_id = str(row.review_id)
        review_content = str(row.review_content).strip()

        prompt = build_prompt(review_content, review_id)

        request_item = {
            "key": review_id,
            "request": {
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": prompt}]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.0,
                    "responseMimeType": "application/json"
                }
            }
        }

        current_file.write(json.dumps(request_item, ensure_ascii=False) + "\n")
        current_count += 1
        total_count += 1

        if current_count >= BATCH_SIZE:
            current_file.close()
            request_files.append(str(current_path))

            batch_index += 1
            current_count = 0
            current_path, current_file = open_new_file(batch_index)

    current_file.close()

    if current_count > 0:
        request_files.append(str(current_path))
    else:
        current_path.unlink(missing_ok=True)

    if not request_files:
        raise ValueError("沒有可分析的評論資料")

    print(f"✅ 共建立 {len(request_files)} 個 JSONL 批次檔，總筆數 {total_count}")

    context["ti"].xcom_push(key="request_files", value=request_files)
    context["ti"].xcom_push(key="total_count", value=total_count)


def normalize_job_state(state):
    if hasattr(state, "name"):
        return state.name
    return str(state)


def is_success_state(state):
    return state in ["SUCCEEDED", "JOB_STATE_SUCCEEDED"]


def is_failed_state(state):
    return state in [
        "FAILED",
        "CANCELLED",
        "EXPIRED",
        "JOB_STATE_FAILED",
        "JOB_STATE_CANCELLED",
        "JOB_STATE_EXPIRED",
    ]


def get_output_file_name(batch_job):
    print(f"🔎 Batch job 完整資訊：{batch_job}")

    if hasattr(batch_job, "dest") and batch_job.dest:
        if hasattr(batch_job.dest, "file_name") and batch_job.dest.file_name:
            return batch_job.dest.file_name

    if hasattr(batch_job, "destination") and batch_job.destination:
        if hasattr(batch_job.destination, "file_name") and batch_job.destination.file_name:
            return batch_job.destination.file_name

    if hasattr(batch_job, "output_file") and batch_job.output_file:
        return batch_job.output_file

    raise ValueError("找不到 Batch output file")


def extract_json_from_response_line(line):
    try:
        data = json.loads(line)
    except Exception as e:
        return {
            "review_id": None,
            "error": "output_line_json_parse_failed",
            "exception": str(e),
            "raw_line": line[:1000]
        }

    key = data.get("key")

    if "error" in data:
        return {
            "review_id": key,
            "error": data["error"]
        }

    response = data.get("response", {})
    candidates = response.get("candidates", [])

    if not candidates:
        return {
            "review_id": key,
            "error": "no_candidates",
            "raw": data
        }

    parts = candidates[0].get("content", {}).get("parts", [])

    if not parts:
        return {
            "review_id": key,
            "error": "no_parts",
            "raw": data
        }

    text = parts[0].get("text", "").strip()

    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(text)

        if "review_id" not in parsed:
            parsed["review_id"] = key

        return parsed

    except Exception:
        return {
            "review_id": key,
            "error": "json_parse_failed",
            "raw_text": text
        }


def process_batches_sequentially(**context):
    from google.cloud import storage
    from google.genai import errors

    print("🎬 Task 2: 逐批建立 Batch、等待完成、下載、清理、上傳 GCS")

    client = init_gemini_client()
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    ti = context["ti"]

    request_files = ti.xcom_pull(
        task_ids="extract_and_prepare_reviews",
        key="request_files"
    )

    if not request_files:
        raise ValueError("XCom 找不到 request_files")

    uploaded_gcs_paths = []

    for batch_index, local_file in enumerate(request_files, start=1):
        gcs_blob_name = (
            f"{GCS_FOLDER}/clean_batch_results/"
            f"{RUN_ID}/batch_{batch_index:04d}_clean.jsonl"
        )

        blob = bucket.blob(gcs_blob_name)
        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_blob_name}"

        if blob.exists():
            print(f"⏭️ 第 {batch_index} 批已存在 GCS，跳過：{gcs_uri}")
            uploaded_gcs_paths.append(gcs_uri)
            continue

        print("======================================")
        print(f"🚀 開始處理第 {batch_index}/{len(request_files)} 批")
        print(f"📄 local_file = {local_file}")
        print(f"🎯 gcs_uri = {gcs_uri}")
        print("======================================")

        uploaded_file = client.files.upload(
            file=local_file,
            config={"mime_type": "application/jsonl"}
        )

        while uploaded_file.state.name != "ACTIVE":
            print(f"等待檔案 ACTIVE，目前：{uploaded_file.state.name}")
            time.sleep(5)
            uploaded_file = client.files.get(name=uploaded_file.name)

        print(f"✅ 檔案 ACTIVE：{uploaded_file.name}")

        while True:
            try:
                batch_job = client.batches.create(
                    model=MODEL_NAME,
                    src=uploaded_file.name,
                    config={
                        "display_name": f"restaurant-review-batch-{RUN_ID}-{batch_index:04d}"
                    }
                )

                print(f"🚀 Batch 建立成功：{batch_job.name}")
                break

            except errors.ClientError as e:
                error_text = str(e)

                if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
                    print("⚠️ 遇到 429 RESOURCE_EXHAUSTED")
                    print(f"⏳ 等待 {CREATE_BATCH_RETRY_SECONDS} 秒後重試")
                    time.sleep(CREATE_BATCH_RETRY_SECONDS)
                    continue

                raise

        job_name = batch_job.name

        while True:
            job = client.batches.get(name=job_name)
            state = normalize_job_state(job.state)

            print(f"⏰ Job {job_name} 狀態：{state}")

            if is_success_state(state):
                print(f"✅ Job 成功完成：{job_name}")
                break

            if is_failed_state(state):
                raise RuntimeError(
                    f"❌ Batch 任務失敗：{job_name}, state={state}, job={job}"
                )

            time.sleep(POLL_SECONDS)

        output_file_name = get_output_file_name(job)

        safe_job_name = job_name.replace("/", "_")
        raw_path = LOCAL_DIR / f"batch_{batch_index:04d}_{safe_job_name}_raw.jsonl"
        clean_path = LOCAL_DIR / f"batch_{batch_index:04d}_clean.jsonl"

        try:
            client.files.download(
                file=output_file_name,
                download_path=str(raw_path)
            )
        except TypeError:
            output_bytes = client.files.download(file=output_file_name)

            with open(raw_path, "wb") as f:
                f.write(output_bytes)

        print(f"✅ 已下載結果檔：{raw_path}")

        success_count = 0
        error_count = 0

        with open(raw_path, "rb") as infile, \
             open(clean_path, "w", encoding="utf-8") as outfile:

            for raw_line in infile:
                line = raw_line.decode("utf-8", errors="replace")

                if not line.strip():
                    continue

                result = extract_json_from_response_line(line)

                outfile.write(json.dumps(result, ensure_ascii=False) + "\n")

                if "error" in result:
                    error_count += 1
                else:
                    success_count += 1

        print(f"✅ 清理完成：成功 {success_count} 筆，錯誤 {error_count} 筆")

        blob.upload_from_filename(clean_path)

        print(f"📤 已上傳：{gcs_uri}")

        uploaded_gcs_paths.append(gcs_uri)

        try:
            raw_path.unlink(missing_ok=True)
            clean_path.unlink(missing_ok=True)
        except Exception as e:
            print(f"⚠️ 清理暫存檔失敗：{e}")

        print(f"✅ 第 {batch_index}/{len(request_files)} 批完成")

    ti.xcom_push(key="clean_result_gcs_paths", value=uploaded_gcs_paths)

    print("🎉 全部批次處理完成")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 26),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "gemini_sentiment_batch_analysis",
    default_args=default_args,
    description="Gemini Developer API Batch 餐廳評論多面向情感分析",
    schedule_interval=None,
    catchup=False,
    tags=["gemini", "batch", "sentiment", "gcs"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_and_prepare_reviews",
        python_callable=extract_and_prepare_reviews,
    )

    t2 = PythonOperator(
        task_id="process_batches_sequentially",
        python_callable=process_batches_sequentially,
        execution_timeout=timedelta(hours=48),
    )

    t1 >> t2