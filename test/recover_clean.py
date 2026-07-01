from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from google import genai
from google.cloud import storage


BUCKET_NAME = "my-airflow-data-bucket-2026"
GCS_PREFIX = "analysis/recovered_cleaned"

TARGET_BATCH_NAMES = [
    "batches/g83l3tls52ncm3s4uhtqoosda2tsa4aeatmi",
    "batches/ldr6mie6abybkx80rvwo4bv2ndyotivh9ezf",
    "batches/4y8ih9guj07m0k5zyteps3fgygjfmzlxg1vn",
    "batches/z2wg4ogkyyzgigxsx4h6fix5p28amfeqn4fa",
    "batches/vdzh49hl54sdsvzcmgyd201xfa29wstd4vuw",
    "batches/n85sde29s2xrz0oq74uyo0mn540urn8179w7",
    "batches/8waxytz3bwwbup8lpoplgq9iqfodl1e91u4v",
    "batches/ba9mxcuszc4k4a0gxss3b2oppekdp8a0q575",
    "batches/8vh97rcp48d7qjgjpjll3w7x962lduydmouf",
    "batches/rv82naa2e3lverqcr6ofbnbs90tcxh50gbuk",
    "batches/2va3ykva9ayyybbdlzsepl2hif7cbtioqewk",
    "batches/yx23s1hl1chrvvu3f272y7zgo0ewggcdg1n3",
]


def upload_to_gcs(local_path: Path, bucket_name: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    print(f"✅ Uploaded: gs://{bucket_name}/{gcs_path}")


def extract_json_from_text(text: str) -> dict[str, Any]:
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def parse_batch_line(raw_line: str) -> dict[str, Any] | None:
    raw = json.loads(raw_line)

    review_key = raw.get("key")

    response = raw.get("response", {})
    candidates = response.get("candidates", [])
    if not candidates:
        return {
            "review_id": review_key,
            "error": "NO_CANDIDATES",
            "raw": raw,
        }

    parts = (
        candidates[0]
        .get("content", {})
        .get("parts", [])
    )

    if not parts:
        return {
            "review_id": review_key,
            "error": "NO_PARTS",
            "raw": raw,
        }

    text = parts[0].get("text", "")
    parsed = extract_json_from_text(text)

    if "review_id" not in parsed:
        parsed["review_id"] = review_key

    return parsed


def clean_batch_jsonl(raw_path: Path, cleaned_path: Path) -> tuple[int, int]:
    success_count = 0
    error_count = 0

    with raw_path.open("r", encoding="utf-8") as fin, cleaned_path.open(
        "w", encoding="utf-8"
    ) as fout:
        for line_no, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                cleaned = parse_batch_line(line)
                if cleaned is None:
                    continue

                fout.write(
                    json.dumps(cleaned, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                )
                success_count += 1

            except Exception as e:
                error_count += 1
                error_record = {
                    "line_no": line_no,
                    "error": str(e),
                    "raw_line": line,
                }
                fout.write(
                    json.dumps(error_record, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                )

    return success_count, error_count


@dag(
    dag_id="recover_gemini_batch_results_cleaned",
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    tags=["gemini", "batch", "recovery"],
)
def recover_gemini_batch_results_cleaned_dag():

    @task
    def recover_and_clean_batch_results():
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("找不到 GEMINI_API_KEY，請確認 Airflow 有設定 GEMINI_API_KEY")

        client = genai.Client(api_key=api_key)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            for batch_name in TARGET_BATCH_NAMES:
                print("-" * 80)
                print(f"處理 Batch: {batch_name}")

                job = client.batches.get(name=batch_name)
                state_name = job.state.name if job.state else ""

                if state_name != "JOB_STATE_SUCCEEDED":
                    print(f"⚠️ Batch 尚未成功，跳過：{batch_name}, state={state_name}")
                    continue

                dest = getattr(job, "dest", None)
                file_name = getattr(dest, "file_name", None) if dest else None

                if not file_name:
                    print(f"⚠️ 找不到結果 file_name，跳過：{batch_name}")
                    continue

                batch_id = batch_name.replace("batches/", "")
                raw_path = tmpdir_path / f"{batch_id}_raw.jsonl"
                cleaned_path = tmpdir_path / f"{batch_id}_cleaned.jsonl"

                print(f"下載 File API 結果：{file_name}")
                file_obj = client.files.download(file=file_name)

                if isinstance(file_obj, bytes):
                    raw_path.write_bytes(file_obj)
                else:
                    raw_path.write_text(str(file_obj), encoding="utf-8")

                print(f"清理 JSONL：{raw_path.name}")
                success_count, error_count = clean_batch_jsonl(raw_path, cleaned_path)

                print(f"✅ 清理完成 success={success_count}, error={error_count}")

                gcs_path = f"{GCS_PREFIX}/{batch_id}.jsonl"
                upload_to_gcs(cleaned_path, BUCKET_NAME, gcs_path)

        print("🎉 全部 Batch 結果已清理並上傳完成")

    recover_and_clean_batch_results()


recover_gemini_batch_results_cleaned_dag()