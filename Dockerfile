# 使用官方 Airflow 2.10+ 作為基底映像檔
FROM apache/airflow:2.10.0-python3.10

# 切換至 root 使用者以調整檔案權限
USER root

# 建立暫存目錄，並確保 airflow 使用者有完整讀寫權限 (對應 LOCAL_REQUEST_FILE 等暫存路徑)
RUN mkdir -p /tmp && chmod 777 /tmp

# 切換回 airflow 使用者進行套件安裝
USER airflow

# 複製本地的 requirements.txt 並使用 pip 安裝
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt