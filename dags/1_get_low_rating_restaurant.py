import json
import os
import glob
from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

# 1. 設定 GCP 與 BigQuery 相關變數
PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"
TABLE_ID = "restaurant"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# 2. 定義 Airflow 預設參數
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}

# 3. 宣告 DAG
with DAG(
    dag_id='to_low_rating_restaurant_bq',
    default_args=default_args,
    schedule_interval=None,  # 手動觸發
    catchup=False,
    tags=['gcp', 'bigquery', 'etl', 'load'],
) as dag:

    # ==========================================
    # 💡 任務一：讀取本地 JSON、清洗並匯入 BigQuery
    # ==========================================
    @task(task_id="json_raw_data_to_bq")
    def run_etl_pipeline():
        data_folder = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'data', 'raw')
        json_files = glob.glob(os.path.join(data_folder, '*.json'))

        if not json_files:
            print(f"⚠️ 找不到 JSON 檔案，請檢查路徑是否存在: {data_folder}")
            return

        insert_data_list = []

        taipei_districts = [
            "中正區", "大同區", "中山區", "松山區", "大安區", "萬華區",
            "信義區", "士林區", "北投區", "內湖區", "南港區", "文山區"
        ]
        
        excluded_categories = ["便利店", "停車場", "人壽保險代理", "軍醫院", "池塘", "公司", "汽車旅館"]
        
        restaurant_set = set()

        for file_path in json_files:
            file_name = os.path.basename(file_path)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    continue

            for item in data:
                restaurant_id = item.get('placeId')
                restaurant_name = item.get('title')
                avg_score = item.get('totalScore')
                reviews_count = item.get('reviewsCount')
                state_raw = item.get('state') or ''
                state = state_raw[-3:] if len(state_raw) >= 3 else state_raw
                category_name = item.get('categoryName')
                google_map_url = item.get('url')
                
                # 假設這裡你已經在前面的步驟中把來源表的 created_at 處理好了
                # 如果來源 JSON 沒有，通常這裡會加上當前時間當作 created_at 塞入
                
                if (restaurant_id and str(restaurant_id).strip() and 
                    restaurant_name and str(restaurant_name).strip() and 
                    state and state.strip() and 
                    google_map_url and str(google_map_url).strip()):
                    
                    clean_id = str(restaurant_id).strip()
                    if (clean_id in restaurant_set) or (state not in taipei_districts) or (category_name in excluded_categories):
                        continue 
                        
                    restaurant_set.add(clean_id)
                    
                    record = {
                        "restaurant_id": clean_id,
                        "restaurant_name": str(restaurant_name).strip(),
                        "avg_score": float(avg_score) if avg_score is not None else None,
                        "reviews_count": int(reviews_count) if reviews_count is not None else None,
                        "state": str(state).strip(),
                        "category_name": str(category_name)[:10].strip() if category_name else None,
                        "google_map_url": str(google_map_url).strip(),
                        "created_at": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f %Z')
                    }
                    insert_data_list.append(record)

        if not insert_data_list:
            return

        client = bigquery.Client(project=PROJECT_ID)
        try:
            errors = client.insert_rows_json(FULL_TABLE_ID, insert_data_list)
            if errors != []:
                raise Exception(f"BigQuery 寫入錯誤: {errors}")
        except Exception as e:
            raise e

    # 💡 實例化第一個任務物件
    task_1_etl = run_etl_pipeline()


    # ==========================================
    # 💡 任務二：從大表中篩選出低評分餐廳，寫入低評分餐廳表
    # ==========================================
    task_2_analysis = BigQueryInsertJobOperator(
        task_id="extract_low_rating_restaurants",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.low_rating_restaurant`
                    (
                        restaurant_id, restaurant_name, avg_score, reviews_count, 
                        state, category_name, google_map_url, 
                        created_at, updated_at
                    )
                    
                    SELECT 
                        restaurant_id, restaurant_name, avg_score, reviews_count, 
                        state, category_name, google_map_url,
                        CURRENT_TIMESTAMP() AS created_at,                    
                        CURRENT_TIMESTAMP() AS updated_at  
                    FROM 
                        `{PROJECT_ID}.{DATASET_ID}.restaurant`
                    WHERE 
                        avg_score <= 3.5
                        AND reviews_count >= 100
                """,
                "useLegacySql": False,
                }
            },
        )


    # ==========================================
    # 💡 5. 關鍵：用 >> 設定任務的執行依賴關係
    # ==========================================
    # 告訴 Airflow：必須等 task_1_etl 成功跑完，才能觸發 task_2_analysis
    task_1_etl >> task_2_analysis