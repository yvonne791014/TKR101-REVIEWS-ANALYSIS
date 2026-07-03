from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = "taipei-restaurant-analysis"
DATASET_ID = "REVIEW"
LOCATION = "asia-east1"

DAG_ID = "create_job_target_discard_reviews"


SQL = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.job_target_discard_reviews` AS

WITH tmp AS (
  -- 1. 撈出不存在食物面向的評論
  SELECT DISTINCT t1.review_id
  FROM `{PROJECT_ID}.{DATASET_ID}.reviews_analysis` t1
  WHERE NOT EXISTS (
    SELECT 1
    FROM `{PROJECT_ID}.{DATASET_ID}.reviews_analysis` t2
    WHERE t2.review_id = t1.review_id
      AND t2.issue_type_id = 1
  )
)

SELECT
  tmp.review_id,
  r.review_score,
  r.restaurant_id
FROM tmp
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.reviews` r
ON tmp.review_id = r.review_id
WHERE r.review_score <= 3

UNION DISTINCT

-- 2. 撈出有談到食物面向且正向的評論
SELECT
  a.review_id,
  r.review_score,
  r.restaurant_id
FROM `{PROJECT_ID}.{DATASET_ID}.reviews_analysis` a
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.reviews` r
ON r.review_id = a.review_id
WHERE a.issue_type_id = 1
  AND a.sentiment = 1
  AND r.review_score <= 3
"""


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 7, 3),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "review", "target_discard_reviews"],
) as dag:

    create_job_target_discard_reviews = BigQueryInsertJobOperator(
        task_id="create_job_target_discard_reviews",
        location=LOCATION,
        configuration={
            "query": {
                "query": SQL,
                "useLegacySql": False,
            }
        },
    )