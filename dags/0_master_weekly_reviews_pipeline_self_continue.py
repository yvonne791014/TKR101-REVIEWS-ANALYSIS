from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

# ============================================================
# Master DAG
# ============================================================
# 目的：
# 1) 手動觸發 DAG0。
# 2) 第一輪：DAG1 -> DAG2 -> DAG3 -> 檢查 status.json。
# 3) 若 status != IMPORTED，等待 5 分鐘後觸發下一輪 DAG0。
# 4) 下一輪 DAG0 會略過 DAG1，只執行 DAG2 -> DAG3。
# 5) 直到 DAG3 將 status.json 更新為 IMPORTED，才執行 DAG4 並結束。
#
# 重點：
# - 不使用 Python while。
# - 不使用 time.sleep。
# - 不在同一個 DAG Run 內展開 72 輪 task。
# - 每一輪都是獨立 DAG0 Run，Airflow UI 會比較乾淨。
# - DAG2 與 DAG3 每輪成對執行，因此每日執行次數相同。
# ============================================================

DAG_ID = "0_master_weekly_reviews_pipeline"
GCP_CONN_ID = "google_cloud_default"
BUCKET_NAME = "my-airflow-data-bucket-2026"
STATUS_BLOB = "analysis_final/status.json"

DAG1_ID = "1_weekly_reviews_gcs_to_bigquery_upsert"
DAG2_ID = "2_gemini_sentiment_batch_analysis_resumable"
DAG3_ID = "3_update_reviews_analysis"
DAG4_ID = "4_create_job_target_discard_reviews"

POLL_INTERVAL_MINUTES = 5
MAX_POLL_ROUNDS = 72
WAIT_POKE_INTERVAL_SECONDS = 30


def _read_status() -> dict[str, Any] | None:
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    if not gcs_hook.exists(bucket_name=BUCKET_NAME, object_name=STATUS_BLOB):
        return None

    raw_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=STATUS_BLOB)
    text = raw_data.decode("utf-8") if isinstance(raw_data, bytes) else raw_data
    return json.loads(text)


def get_round_index(**context) -> int:
    dag_run = context.get("dag_run")
    if not dag_run or not dag_run.conf:
        return 1
    return int(dag_run.conf.get("round_index", 1) or 1)


def should_run_dag1(**context) -> str:
    """
    第一輪手動觸發 DAG0 時執行 DAG1。
    後續由 DAG0 自己觸發的輪詢 run，conf[continue_pipeline]=True，會略過 DAG1。
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    continue_pipeline = bool(conf.get("continue_pipeline", False))
    round_index = int(conf.get("round_index", 1) or 1)

    print(f"continue_pipeline={continue_pipeline}, round_index={round_index}")

    if continue_pipeline:
        return "skip_dag1"

    return "trigger_dag1"


def branch_after_dag3(**context) -> str:
    """
    DAG3 每輪執行完後檢查 status.json：
    - IMPORTED：觸發 DAG4。
    - 非 IMPORTED：若未超過最大輪數，等待 5 分鐘後觸發下一輪 DAG0。
    """
    round_index = get_round_index(**context)
    status_payload = _read_status()

    print(f"round_index={round_index}")

    if status_payload:
        print("目前 status.json：")
        print(json.dumps(status_payload, ensure_ascii=False, indent=2))
        status = str(status_payload.get("status", "")).upper()
    else:
        print("找不到 status.json，視為尚未完成匯入。")
        status = ""

    if status == "IMPORTED":
        return "trigger_dag4"

    if round_index >= MAX_POLL_ROUNDS:
        return "polling_timeout"

    return "wait_5_minutes_before_next_round"


def raise_polling_timeout(**context) -> None:
    round_index = get_round_index(**context)
    status_payload = _read_status()
    raise AirflowFailException(
        "DAG2 + DAG3 polling timeout. "
        f"round_index={round_index}, MAX_POLL_ROUNDS={MAX_POLL_ROUNDS}, "
        f"最後 status.json={status_payload}"
    )


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Master DAG: DAG1 once, then paired DAG2/DAG3 self-continue polling until IMPORTED, then DAG4",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["gemini", "master", "pipeline", "reviews", "weekly"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch_run_dag1 = BranchPythonOperator(
        task_id="branch_run_dag1",
        python_callable=should_run_dag1,
    )

    trigger_dag1 = TriggerDagRunOperator(
        task_id="trigger_dag1",
        trigger_dag_id=DAG1_ID,
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=WAIT_POKE_INTERVAL_SECONDS,
        reset_dag_run=True,
        trigger_run_id="master__{{ ts_nodash }}__dag1",
        conf={"triggered_by": DAG_ID, "step": "dag1"},
    )

    skip_dag1 = EmptyOperator(task_id="skip_dag1")

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id=DAG2_ID,
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=WAIT_POKE_INTERVAL_SECONDS,
        reset_dag_run=True,
        trigger_run_id="master__{{ ts_nodash }}__dag2_r{{ dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1 }}",
        conf={
            "triggered_by": DAG_ID,
            "round_index": "{{ dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1 }}",
            "step": "dag2",
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    trigger_dag3 = TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id=DAG3_ID,
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=WAIT_POKE_INTERVAL_SECONDS,
        reset_dag_run=True,
        trigger_run_id="master__{{ ts_nodash }}__dag3_r{{ dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1 }}",
        conf={
            "triggered_by": DAG_ID,
            "round_index": "{{ dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1 }}",
            "step": "dag3",
        },
    )

    check_import_status = BranchPythonOperator(
        task_id="check_import_status",
        python_callable=branch_after_dag3,
    )

    trigger_dag4 = TriggerDagRunOperator(
        task_id="trigger_dag4",
        trigger_dag_id=DAG4_ID,
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=WAIT_POKE_INTERVAL_SECONDS,
        reset_dag_run=True,
        trigger_run_id="master__{{ ts_nodash }}__dag4",
        conf={
            "triggered_by": DAG_ID,
            "round_index": "{{ dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1 }}",
            "step": "dag4",
        },
    )

    wait_5_minutes_before_next_round = TimeDeltaSensor(
        task_id="wait_5_minutes_before_next_round",
        delta=timedelta(minutes=POLL_INTERVAL_MINUTES),
        mode="reschedule",
    )

    trigger_next_round = TriggerDagRunOperator(
        task_id="trigger_next_round",
        trigger_dag_id=DAG_ID,
        wait_for_completion=False,
        reset_dag_run=False,
        trigger_run_id="master_continue__{{ ts_nodash }}__r{{ ((dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1) | int) + 1 }}",
        conf={
            "continue_pipeline": True,
            "round_index": "{{ ((dag_run.conf.get('round_index', 1) if dag_run and dag_run.conf else 1) | int) + 1 }}",
            "triggered_by": DAG_ID,
        },
    )

    polling_timeout = PythonOperator(
        task_id="polling_timeout",
        python_callable=raise_polling_timeout,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> branch_run_dag1
    branch_run_dag1 >> trigger_dag1 >> trigger_dag2
    branch_run_dag1 >> skip_dag1 >> trigger_dag2

    trigger_dag2 >> trigger_dag3 >> check_import_status

    check_import_status >> trigger_dag4 >> end
    check_import_status >> wait_5_minutes_before_next_round >> trigger_next_round >> end
    check_import_status >> polling_timeout
