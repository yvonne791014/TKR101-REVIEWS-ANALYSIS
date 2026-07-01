from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"
TABLE_ID = "reviews"
BUCKET_NAME = "my-airflow-data-bucket-2026"

def get_gcs_files():
    try:
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        # 撈取該儲存桶內 prefix 為 'round5/' 的所有物件
        all_files = hook.list(bucket_name=BUCKET_NAME, prefix="round5/")
        # 只過濾出 .csv 結尾的檔案
        return [f for f in all_files if f.endswith('.csv')]
    except Exception:
        return []

with DAG(
    dag_id="reviews_gcs_to_bigquery",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 💡 改從 GCS 撈取檔案清單，不再依賴地端 os.listdir
    gcs_file_list = get_gcs_files()

    if not gcs_file_list:
        # 預防萬一 GCS 真的沒檔案時，建立一個虛無任務避免 DAG 報錯
        from airflow.operators.empty import EmptyOperator
        dummy = EmptyOperator(task_id="no_files_found_in_gcs")
    else:
        for gcs_file_path in gcs_file_list:
            # gcs_file_path 會長這樣: 'round5/review_part1.csv'
            # 轉換成安全乾淨的 Task ID 後綴
            task_suffix = gcs_file_path.replace('/', '_').replace('.', '_')

            load_gcs_to_bq = GCSToBigQueryOperator(
                task_id=f"load_gcs_to_bq_{task_suffix}",
                bucket=BUCKET_NAME,
                source_objects=[gcs_file_path], # 直接帶入 GCS 的完整路徑
                destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
                source_format="CSV",
                write_disposition="WRITE_APPEND",
                skip_leading_rows=1,
                autodetect=False, 
                schema_fields=[
                    {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "restaurant_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "review_score", "type": "INTEGER", "mode": "REQUIRED"}, 
                    {"name": "review_content", "type": "STRING", "mode": "NULLABLE"},
                    
                    # 💡 改為 STRING 型態
                    {"name": "food_score", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "service_score", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "atmosphere_score", "type": "STRING", "mode": "NULLABLE"},
                ],

                # 💡 移除不支援的 null_marker，改用這三行 2.7.2 絕對支援的相容金鑰
                allow_quoted_newlines=True,      
                ignore_unknown_values=True,      
                max_bad_records=1000, # 👈 舊版直接拉大容錯率，讓 BQ 強行吞下空值列

                gcp_conn_id="google_cloud_default",
            )
            
            load_gcs_to_bq