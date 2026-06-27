import json
import os
import time
from datetime import datetime, timedelta

# 引入 Airflow 相關套件
from airflow import DAG
from airflow.operators.python import PythonOperator

# ================= GCP & 專案全域設定 =================
PROJECT_ID = "taipei-restaurant-analysis"
BUCKET_NAME = "my-airflow-data-bucket-2026"
GCS_FOLDER = "analysis"  # 存放於 analysis 資料夾底下的前綴路徑

# BigQuery 來源資料設定
BQ_DATASET_ID = "REVIEW"
BQ_TABLE_ID = "reviews"

# 本地暫存路徑 (Airflow Worker 的臨時工作空間)
LOCAL_REQUEST_FILE = "/tmp/gemini_batch_requests.jsonl"
LOCAL_RAW_RESULT_FILE = "/tmp/gemini_raw_results.jsonl"
LOCAL_CLEAN_RESULT_FILE = "/tmp/gemini_clean_results.jsonl"
# ====================================================

def init_gemini_client():
    """
    初始化並回傳 Gemini 客戶端
    採用延遲匯入 (Lazy Import) 防止 Airflow 載入 DAG 時因缺少套件而直接 Broken
    """
    try:
        from google import genai
    except ImportError:
        raise ImportError(
            "❌ 載入 'google-genai' 失敗！請確保 Airflow 執行環境（Worker/Scheduler）"
            "已安裝 'google-genai' 套件。請參考 requirements.txt 安裝指南。"
        )
        
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("❌ 錯誤：在環境變數中找不到 'GEMINI_API_KEY'！")
    return genai.Client(api_key=api_key)


def build_prompt(text_content, review_id):
    """組合多面向情感分析 System Prompt"""
    return f"""你是一個精準的餐廳評論多面向情感分析器 (Aspect-Based Sentiment Analysis Analyzer)。
請分析下方提供的餐廳評論，並嚴格按照指定的 JSON 格式輸出。

【分析面向與定義】
- food (食物): 包含口味、食材、好不好吃、多汁、柴等。
- price (價格): 包含划算、貴、便宜、CP值高低等。
- service (服務): 包含服務態度、速度、店員表現等。
- hygiene (衛生): 包含乾淨、髒亂、餐具衛生等。
- queue (排隊): 包含等很久、排隊人潮、排很久等。
- environment (環境): 包含裝潢、氣氛、噪音、擁擠程度等.
- parking (停車): 包含好不好停車、停車場等。

【情感分類標籤與輸出規則】
每個面向的情感 (sentiment) 只能從以下三個標籤中選擇一個，並請嚴格遵守數字轉換與隱藏規則：
- 正面評價：請輸出數字 1
- 負面評價：請輸出數字 -1
- 完全沒有提到該面向："not_mentioned"，且**該面向必須完全從輸出的 JSON 中移除（不需回傳）**。

【關鍵字規則】
在 keywords 陣列中，只填入評論中與該面向直接相關的「原始中文字詞或短句」。

【輸入資料】
- 評論 ID: "{review_id}"
- 原始評論內容: "{text_content}"

【輸出的 JSON 格式樣本】
{{
  "review_id": "{review_id}",
  "food": {{ "sentiment": -1, "keywords": ["東西不怎麼樣"] }},
  "price": {{ "sentiment": -1, "keywords": ["價格貴"] }},
  "service": {{ "sentiment": 1, "keywords": ["服務好"] }},
  "queue": {{ "sentiment": 1, "keywords": ["上菜速度快"] }}
}}

【重要注意事項】
1. 請務必將 JSON 中的 "review_id" 欄位值，替換為上方【輸入資料】中實際傳入的 "{review_id}" 變數內容，不可沿用樣本中的字串。
2. 輸出時請只回傳標準 JSON 格式字串，不要包含 any markdown 標籤（如 ```json）或額外的解釋文字。"""


# ====================================================
#              Airflow Task 函式定義
# ====================================================

def extract_and_prepare_reviews(**context):
    """
    Task 1: 從 BigQuery 撈取低星評論並轉換成 JSONL 批次請求檔
    """
    print("📋 開始執行 Task 1: 從 BigQuery 撈取評論並製作 JSONL 檔")
    
    # 延遲匯入 google-cloud-bigquery 套件，防止 DAG 解析時報錯
    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError(
            "❌ 載入 'google-cloud-bigquery' 失敗！請確保 Airflow 執行環境（Worker/Scheduler）"
            "已安裝 'google-cloud-bigquery' 套件。"
        )

    # 初始化 BigQuery 客戶端（在 GCP 環境下，其會自動尋找環境憑證與預設專案）
    bq_client = bigquery.Client(project=PROJECT_ID)

    # 針對 BigQuery 特性改寫的 Standard SQL
    # 【測試階段控制】：SQL 語句末尾加上 LIMIT 10，正式執行 13 萬筆時可以將 LIMIT 10 移除
    sql_query = f"""
        SELECT review_id, restaurant_id, review_score, review_content
        FROM `{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
        WHERE review_score <= 3 AND review_content IS NOT NULL
        LIMIT 10;
    """
    
    print(f"🔍 執行 BigQuery 查詢：\n{sql_query}")
    query_job = bq_client.query(sql_query)
    results = query_job.result()  # 等待查詢完成

    reviews_data = []
    for row in results:
        reviews_data.append({
            "review_id": row.review_id,
            "restaurant_id": row.restaurant_id,
            "review_score": row.review_score,
            "review_content": row.review_content
        })

    valid_reviews = [
        r for r in reviews_data 
        if r.get("review_content") and r.get("review_content").strip() != ""
    ]
    
    print(f"📊 從 BigQuery 成功獲取 {len(valid_reviews)} 筆有效低星評論資料。")

    if not valid_reviews:
        raise ValueError("❌ 找不到任何有效的評論資料（可能皆為空字串），無法建立批次請求。")

    print(f"✍️ 正在寫入本地暫存檔案: {LOCAL_REQUEST_FILE}...")
    with open(LOCAL_REQUEST_FILE, "w", encoding="utf-8") as f:
        for review in valid_reviews:
            prompt_content = build_prompt(review["review_content"], review["review_id"])
            request_item = {
                "contents": prompt_content,
                "config": {
                    "response_mime_type": "application/json",
                    "temperature": 0.0
                }
            }
            f.write(json.dumps(request_item, ensure_ascii=False) + "\n")
            
    print(f"✅ 成功將 {len(valid_reviews)} 筆請求資料寫入 {LOCAL_REQUEST_FILE}。")


def trigger_gemini_batch_job(**context):
    """
    Task 2: 使用正規 SDK 流程上傳檔案並啟動 Batch 任務 (指定輸出 GCS)
    """
    print("🎬 開始執行 Task 2: 啟動 Gemini Batch 任務")
    client = init_gemini_client()
    
    if not os.path.exists(LOCAL_REQUEST_FILE):
        raise FileNotFoundError(f"❌ 找不到請求檔案：{LOCAL_REQUEST_FILE}")
        
    print("⏳ 1. 正在將檔案上傳至 Gemini 檔案暫存區...")
    # 這是標準寫法，上傳後會得到一個完整的文件物件
    uploaded_file = client.files.upload(
        file=LOCAL_REQUEST_FILE,
        config=dict(mime_type="text/plain")
    )
    print(f"🚀 檔案上傳成功，遠端名稱: {uploaded_file.name}")
    
    print("🤖 2. 建立 Batch 預測任務...")
    # 💡 關鍵修正：將 dst 改為 output_uri
    batch_job = client.batches.create(
        model='gemini-2.5-flash',
        src=uploaded_file,
        output_uri=f"gs://{BUCKET_NAME.lower()}/{GCS_FOLDER}"
    )
    
    print(f"🎉 任務啟動成功！")
    print(f"   - Job ID: {batch_job.name}")
    print(f"   - 狀態: {batch_job.state}")
    
    context['ti'].xcom_push(key='gemini_batch_job_name', value=batch_job.name)

def monitor_and_upload_results(**context):
    """
    Task 3: 監控 Batch 任務，完成後下載、提取 Prompt 格式 JSON 並上傳至指定 GCS
    """
    print("📬 開始執行 Task 3: 監控任務進度與上傳至 GCS")
    client = init_gemini_client()
    
    ti = context['ti']
    batch_job_name = ti.xcom_pull(task_ids='trigger_gemini_batch_job', key='gemini_batch_job_name')
    if not batch_job_name:
        raise ValueError("❌ 錯誤：無法從 XComs 獲取 'gemini_batch_job_name'！")

    while True:
        job_status = client.batches.get(name=batch_job_name)
        print(f"⏰ [{time.strftime('%Y-%m-%d %H:%M:%S')}] "
              f"狀態: {job_status.state} | "
              f"成功筆數: {job_status.completed_request_count}")
        
        if job_status.state == "SUCCEEDED":
            print("\n🏆 Gemini 批次任務分析成功完成！")
            break
        elif job_status.state in ["FAILED", "CANCELLED"]:
            raise RuntimeError(f"❌ Gemini Batch 任務失敗或被取消，最終狀態：{job_status.state}")
            
        time.sleep(30)

    # 💡 關鍵修正：最新版 SDK 的 output_file 回傳的是 GCS URI (gs://...)
    # 我們不使用 client.files.download，改用標準的 google-cloud-storage 來下載結果
    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError("❌ 載入 'google-cloud-storage' 失敗！") from e

    print(f"📥 正在從 GCS 下載原始回傳結果... 來源路徑: {job_status.output_file}")
    
    # 解析 gs:// 字串以取得 bucket 名稱與路徑
    # 格式範例: gs://gcs-bucket-name/path/to/output.jsonl
    raw_uri = job_status.output_file
    if raw_uri.startswith("gs://"):
        uri_path = raw_uri[5:]
        output_bucket_name = uri_path.split('/')[0]
        output_blob_name = '/'.join(uri_path.split('/')[1:])
    else:
        raise ValueError(f"無法解析的回傳路徑格式: {raw_uri}")

    storage_client = storage.Client(project=PROJECT_ID)
    out_bucket = storage_client.bucket(output_bucket_name)
    out_blob = out_bucket.blob(output_blob_name)
    
    raw_downloaded_path = "/tmp/gemini_raw_output.jsonl"
    out_blob.download_from_filename(raw_downloaded_path)

    print("🧹 正在剝除 Gemini API 外殼，還原為 Prompt 要求的巢狀 JSON 格式...")
    
    with open(raw_downloaded_path, "r", encoding="utf-8") as infile, \
         open(LOCAL_CLEAN_RESULT_FILE, "w", encoding="utf-8") as outfile:
        
        for line in infile:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                response = data.get("response", {})
                candidates = response.get("candidates", [])
                if not candidates:
                    continue
                
                raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                ai_json = json.loads(raw_text.strip())
                outfile.write(json.dumps(ai_json, ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"⚠️ 警告：還原單行 JSON 結果時發生錯誤（略過該筆）: {e}")

    print("📤 準備上傳還原後的 JSONL 檔案至 GCS...")
    timestamp = int(time.time())
    gcs_blob_name = f"{GCS_FOLDER}/clean_reviews_analysis_{timestamp}.jsonl"
    
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(LOCAL_CLEAN_RESULT_FILE)
    print(f"🎉 檔案成功上傳至 GCS！\n   - 儲存路徑: gs://{BUCKET_NAME}/{gcs_blob_name}")

# ====================================================
#                 Airflow DAG 定義
# ====================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gemini_sentiment_batch_analysis',
    default_args=default_args,
    description='使用 Gemini Batch API 批次情感分析，並將結果輸出至 GCS analysis/',
    schedule_interval=None,  # 預設不自動排程，手動觸發
    catchup=False,
    tags=['gemini', 'sentiment', 'gcs'],
) as dag:

    # Task 1: 準備資料
    t1 = PythonOperator(
        task_id='extract_and_prepare_reviews',
        python_callable=extract_and_prepare_reviews,
    )

    # Task 2: 觸發批次任務
    t2 = PythonOperator(
        task_id='trigger_gemini_batch_job',
        python_callable=trigger_gemini_batch_job,
        provide_context=True,
    )

    # Task 3: 監控進度並上傳至 GCS
    t3 = PythonOperator(
        task_id='monitor_and_upload_results',
        python_callable=monitor_and_upload_results,
        provide_context=True,
        execution_timeout=timedelta(hours=24),  # 設定超時上限為 24 小時（為 Batch 任務留充裕時間）
    )

    # 串接任務相依性流程 (ETL Pipeline)
    t1 >> t2 >> t3