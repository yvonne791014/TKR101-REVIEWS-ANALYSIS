import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="reviews_local_to_gcs",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    LOCAL_DATA_DIR = "/opt/airflow/data/reviews/round5"
    BUCKET_NAME = "my-airflow-data-bucket-2026" # 💡 請確認這是你的儲存桶名稱
    
    try:
        file_list = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.csv')]
    except FileNotFoundError:
        file_list = []

    for file_name in file_list:
        task_suffix = file_name.replace('.', '_')

        upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f"upload_to_gcs_{task_suffix}",
            src=os.path.join(LOCAL_DATA_DIR, file_name), 
            dst=f"round5/{file_name}",
            bucket=BUCKET_NAME,
            gcp_conn_id="google_cloud_default",
        )
        
        # trigger_bq_dag = TriggerDagRunOperator(
        #     task_id="trigger_dag_2",
        #     trigger_dag_id="dag_2_gcs_to_reviews_bq", # 要觸發的第二隻 DAG ID
        # )


        # 獨立運行，不需要下游任務
        upload_to_gcs


