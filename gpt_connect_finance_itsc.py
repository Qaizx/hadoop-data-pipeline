import os
from dotenv import load_dotenv
from openai import OpenAI
from pyhive import hive
import pandas as pd  # เพิ่ม

load_dotenv()

# ตั้งค่า OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ตั้งค่า Hive connection
hive_conn = hive.Connection(
    host="localhost",
    port=10000,
    database="default",
)

# บอก GPT เกี่ยวกับ schema ของ table
TABLE_SCHEMA = """
Table: finance_itsc_long
Columns:
- date (STRING): เดือนของรายการ เช่น '2024-01', '2024-02'
- details (STRING): ชนิดของรายการ เช่น 'budget', 'spent' หรือ 'remaining'
- category (STRING): หมวดหมู่ค่าใช้จ่าย
- amount (DECIMAL): จำนวนเงิน (บาท)
- year (INT): ปีของรายการ เช่น 2024
"""

def ask_gpt_for_sql(user_question: str) -> str:
    """ให้ GPT แปลงคำถามเป็น SQL"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": f"""คุณเป็น SQL expert สำหรับ Hive/Hadoop
ให้แปลงคำถามภาษาไทยเป็น HiveQL query

{TABLE_SCHEMA}

ตอบเฉพาะ SQL query เท่านั้น ไม่ต้องอธิบาย ไม่ต้องใส่ markdown code block
ใช้ syntax ของ HiveQL"""
            },
            {"role": "user", "content": user_question}
        ]
    )
    
    sql = response.choices[0].message.content.strip()
    
    # ลบ markdown code block
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        sql = "\n".join(lines).strip()
    
    # แปลง smart quotes เป็น straight quotes
    sql = sql.replace("'", "'").replace("'", "'")
    sql = sql.replace(""", '"').replace(""", '"')
    
    # ลบ ; ตัวสุดท้าย
    sql = sql.rstrip(";")
    
    return sql

def execute_query(sql: str):
    """รัน SQL query บน Hive แล้ว return list"""
    cursor = hive_conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def execute_query_df(sql: str) -> pd.DataFrame:
    """รัน SQL query บน Hive แล้ว return DataFrame"""
    cursor = hive_conn.cursor()
    cursor.execute(sql)
    
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    
    return pd.DataFrame(data, columns=columns)

def ask_gpt_to_summarize(question: str, sql: str, results) -> str:
    """ให้ GPT สรุปผลลัพธ์"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": f"""คำถาม: {question}

SQL ที่ใช้: {sql}

ผลลัพธ์: {results}

กรุณาสรุปคำตอบเป็นภาษาไทยให้เข้าใจง่าย"""}
        ]
    )
    
    return response.choices[0].message.content.strip()

def suggest_chart_type(question: str, df: pd.DataFrame) -> str:
    """ให้ GPT แนะนำประเภท chart"""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """วิเคราะห์คำถามและข้อมูล แล้วแนะนำประเภท chart ที่เหมาะสม
ตอบเพียงคำเดียว: bar, line, pie, none"""
            },
            {"role": "user", "content": f"คำถาม: {question}\nColumns: {df.columns.tolist()}\nRows: {len(df)}"}
        ]
    )
    
    return response.choices[0].message.content.strip().lower()

# ============ Main ============
def chat_with_data(question: str):
    print(f"📝 คำถาม: {question}\n")
    
    # 1. แปลงเป็น SQL
    sql = ask_gpt_for_sql(question)
    
    # 2. รัน query
    results = execute_query(sql)
    
    # 3. สรุปผล
    summary = ask_gpt_to_summarize(question, sql, results)
    print(f"💬 คำตอบ: {summary}")

def chat_with_data_full(question: str) -> dict:
    """ถามคำถามแล้ว return ผลลัพธ์ทั้งหมด (สำหรับ Dashboard)"""
    
    sql = ask_gpt_for_sql(question)
    df = execute_query_df(sql)
    summary = ask_gpt_to_summarize(question, sql, df.to_string())
    chart_type = suggest_chart_type(question, df) if len(df) > 0 else "none"
    
    return {
        "question": question,
        "sql": sql,
        "df": df,
        "summary": summary,
        "chart_type": chart_type
    }

# ทดสอบ
if __name__ == "__main__":
    chat_with_data("งบการเงินปี 2024 นี้ใช้ไปเท่าไร")
    chat_with_data("เปรียบเทียบค่าใช้จ่ายแต่ละหมวดหมู่ในปี 2024")