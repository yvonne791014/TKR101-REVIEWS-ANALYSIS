from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from google import genai
from google.cloud import storage


BUCKET_NAME = "my-airflow-data-bucket-2026"
GCS_PREFIX = "analysis/recovered"
DISPLAY_NAME_PREFIX = "restaurant-review-batch"

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
    print(f"✅ Uploaded to gs://{bucket_name}/{gcs_path}")


@dag(
    dag_id="recover_gemini_batch_results",
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    tags=["gemini", "batch", "recovery"],
)
def recover_gemini_batch_results_dag():

    @task
    def recover_batch_results():
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("找不到 GEMINI_API_KEY，請確認 Airflow 有設定環境變數")

        client = genai.Client(api_key=api_key)

        jobs = []
        if TARGET_BATCH_NAMES:
            for batch_name in TARGET_BATCH_NAMES:
                job = client.batches.get(name=batch_name)
                jobs.append(job)
        else:
            for job in client.batches.list(config={"page_size": 100}):
                display_name = getattr(job, "display_name", "") or ""
                state_name = job.state.name if job.state else ""

                if (
                    display_name.startswith(DISPLAY_NAME_PREFIX)
                    and state_name == "JOB_STATE_SUCCEEDED"
                ):
                    jobs.append(job)

        print(f"找到 {len(jobs)} 個 Batch")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_dir = Path(tmpdir)

            for job in jobs:
                print("-" * 80)
                print("batch:", job.name)
                print("display_name:", getattr(job, "display_name", None))
                print("state:", job.state.name if job.state else None)

                state_name = job.state.name if job.state else ""
                if state_name != "JOB_STATE_SUCCEEDED":
                    print(f"⚠️ 尚未成功，跳過：{job.name}, state={state_name}")
                    continue

                dest = getattr(job, "dest", None)
                file_name = getattr(dest, "file_name", None) if dest else None

                if not file_name:
                    print(f"⚠️ 沒有 file_name，跳過：{job.name}")
                    continue

                batch_id = job.name.replace("batches/", "")
                local_output = local_dir / f"{batch_id}.jsonl"

                print(f"下載結果檔：{file_name}")
                file_obj = client.files.download(file=file_name)

                if isinstance(file_obj, bytes):
                    local_output.write_bytes(file_obj)
                else:
                    local_output.write_text(str(file_obj), encoding="utf-8")

                print(f"✅ Downloaded: {local_output}")

                gcs_path = f"{GCS_PREFIX}/{batch_id}.jsonl"
                upload_to_gcs(local_output, BUCKET_NAME, gcs_path)

        print("🎉 Batch 結果救援完成")

    recover_batch_results()


recover_gemini_batch_results_dag()