from google import genai
from dotenv import load_dotenv
import os

# 用來查看Gemini批次語意分析的 Batch任務進度 / state 狀態

load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise RuntimeError("找不到 GEMINI_API_KEY，請確認 .env 裡有設定")

client = genai.Client(api_key=api_key)

# for job in client.batches.list(config={"page_size": 20}):
#     print("name:", job.name)
#     print("display_name:", getattr(job, "display_name", None))
#     print("state:", job.state.name if job.state else None)
#     print("created:", getattr(job, "create_time", None))
#     print("dest:", getattr(job, "dest", None))
#     print("-" * 80)

for job in client.batches.list(config={"page_size": 100}):
    print(job.name, job.display_name, job.state.name)

# job = client.batches.get(name="batches/g83l3tls52ncm3s4uhtqoosda2tsa4aeatmi")
# print(job)