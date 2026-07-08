# TKR101-REVIEWS-ANALYSIS
題目：孤獨的美食家
副標：洗掉情緒後，這家餐廳其實很好吃
說明：Google Map低評分餐廳評論分析
流程：Google Maps → Reviews 更新 → AI 語意分析 → 分析結果回寫 → 建立需排除評論清單 → 後續重新計算餐廳評分。

                Google Maps
                     │
                     ▼
              每週Crawler產生CSV
                     │
                     ▼
        ┌─────────────────────────┐
        │ DAG1                    │
        │ Reviews 更新            │
        │ • CSV → Staging         │
        │ • MERGE Reviews         │
        │ • 更新餐廳評論數         │
        │ • 建立 Queue(PENDING)   │
        └──────────┬──────────────┘
                   │
                   ▼
        ┌─────────────────────────┐
        │ DAG2                    │
        │ Gemini Batch Analysis   │
        │ • Queue → JSONL         │
        │ • 建立 Batch Job        │
        │ • AI 多面向情緒分析      │
        │ • 產生 Clean JSONL      │
        └──────────┬──────────────┘
                   │
                   ▼
        ┌─────────────────────────┐
        │ DAG3                    │
        │ 寫回 BigQuery           │
        │ • JSONL 解析            │
        │ • 資料清洗              │
        │ • 更新 reviews_analysis │
        └──────────┬──────────────┘
                   │
                   ▼
        ┌─────────────────────────┐
        │ DAG4                    │
        │ 建立排除評論清單         │
        │ • 判斷 Food 面向         │
        │ • 篩選需排除評論         │
        │ • 建立                  │
        │   job_target_discard_   │
        │   reviews               │
        └──────────┬──────────────┘
                   │
                   ▼
        後續重新計算餐廳評分（Discard Reviews）



airflow dag0 流程圖：

        DAG0
│
├── DAG1（一次）
│
├── DAG2
│
├── DAG3
│
├── status != IMPORTED ?
│       │
│       ├── 等5分鐘
│       │
│       └── Trigger 下一輪 DAG0
│
└── status = IMPORTED
        │
        ▼
      DAG4