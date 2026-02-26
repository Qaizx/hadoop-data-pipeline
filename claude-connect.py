import os
from anthropic import Anthropic
from pyhive import hive  # หรือใช้ pyspark ก็ได้

# ตั้งค่า Claude client
# client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# ตั้งค่า Hive connection
hive_conn = hive.Connection(
    host="localhost",
    port=10000,
    database="default",
)

# บอก Claude เกี่ยวกับ schema ของ table
TABLE_SCHEMA = """
Table: finance_long
Columns:
- period_month (STRING): เดือนของรายการ เช่น '2024-01', '2024-02'
- transaction_type (STRING): ชนิดของรายการ เช่น 'expense' หรือ 'income'
- category (STRING): หมวดหมู่ค่าใช้จ่าย
- amount (DECIMAL): จำนวนเงิน (บาท)
- year (INT): ปีของรายการ เช่น 2024
"""

def ask_claude_for_sql(user_question: str) -> str:
    """ให้ Claude แปลงคำถามเป็น SQL"""
    
    message = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=1024,
        system=f"""คุณเป็น SQL expert สำหรับ Hive/Hadoop
ให้แปลงคำถามภาษาไทยเป็น HiveQL query

{TABLE_SCHEMA}

ตอบเฉพาะ SQL query เท่านั้น ไม่ต้องอธิบาย
ใช้ syntax ของ HiveQL""",
        messages=[
            {"role": "user", "content": user_question}
        ]
    )
    
    return message.content[0].text.strip()

def execute_query(sql: str):
    """รัน SQL query บน Hive"""
    cursor = hive_conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def ask_claude_to_summarize(question: str, sql: str, results) -> str:
    """ให้ Claude สรุปผลลัพธ์"""
    
    message = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=1024,
        messages=[
            {"role": "user", "content": f"""คำถาม: {question}

SQL ที่ใช้: {sql}

ผลลัพธ์: {results}

กรุณาสรุปคำตอบเป็นภาษาไทยให้เข้าใจง่าย"""}
        ]
    )
    
    return message.content[0].text

# ============ Main ============
def chat_with_data(question: str):
    print(f"📝 คำถาม: {question}\n")
    
    # 1. แปลงเป็น SQL
    sql = ask_claude_for_sql(question)
    print(f"🔍 SQL Query:\n{sql}\n")
    
    # 2. รัน query
    results = execute_query(sql)
    print(f"📊 Raw Results: {results}\n")
    
    # 3. สรุปผล
    summary = ask_claude_to_summarize(question, sql, results)
    print(f"💬 คำตอบ: {summary}")

# ทดสอบ
if __name__ == "__main__":
    chat_with_data("งบการเงินปีนี้ใช้ไปเท่าไร")
    chat_with_data("แผนกไหนใช้งบเยอะที่สุด")
    chat_with_data("เปรียบเทียบค่าใช้จ่ายแต่ละแผนกในปี 2024")