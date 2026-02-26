# services/hive_gpt.py
import os
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from pyhive import hive
from config import HIVE_DATABASE, GPT_MODEL, TABLE_SCHEMA, CATEGORY_MAPPING

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", 10000))


# ===== Hive =====

def get_hive_connection():
    """สร้าง Hive connection"""
    return hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
    )


def execute_query(sql: str) -> list:
    """รัน SQL query แล้ว return list"""
    conn = get_hive_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.close()
    return result


def execute_query_df(sql: str) -> pd.DataFrame:
    """รัน SQL query แล้ว return DataFrame"""
    conn = get_hive_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data, columns=columns)


# ===== GPT =====

def fix_hive_reserved_keywords(sql: str) -> str:
    """Post-process SQL จาก GPT: ครอบ reserved keywords ด้วย backtick"""
    import re
    sql = re.sub(r'(?<!`)\bdate\b(?!`)', '`date`', sql)
    return sql


def has_bad_remaining_sum(sql: str) -> bool:
    """ตรวจว่า SQL มี SUM(CASE WHEN details='remaining') ซึ่งผิดหลักการ"""
    import re
    pattern = r'SUM\s*\(\s*CASE\s+WHEN\s+\S*details\S*\s*=\s*['"`]remaining['"']'
    return bool(re.search(pattern, sql, re.IGNORECASE))


def ask_gpt_for_sql(user_question: str) -> str:
    """แปลงคำถามภาษาไทยเป็น HiveQL"""
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": f"""คุณเป็น SQL expert สำหรับ Hive/Hadoop
ให้แปลงคำถามภาษาไทยเป็น HiveQL query

{TABLE_SCHEMA}

ข้อห้ามเด็ดขาด:
- ห้าม SUM(amount) WHERE details = 'remaining' ข้ามหลายเดือน
- หากคำถามถามเรื่อง "ยอดคงเหลือ" หรือ "remaining" ให้ดึงเฉพาะเดือนล่าสุดเสมอ

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
    sql = sql.replace("\u2018", "'").replace("\u2019", "'")
    sql = sql.replace("\u201c", '"').replace("\u201d", '"')

    # ลบ ; ตัวสุดท้าย
    sql = sql.rstrip(";")

    # post-process: แก้ reserved keywords ที่ GPT มักลืม
    sql = fix_hive_reserved_keywords(sql)

    return sql


def ask_gpt_to_summarize(question: str, sql: str, results) -> str:
    """สรุปผลลัพธ์เป็นภาษาไทย"""
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": f"""สรุปผลลัพธ์เป็นภาษาไทยให้เข้าใจง่าย
เมื่อพบชื่อ category ภาษาอังกฤษให้แปลกลับเป็นภาษาไทยตาม mapping นี้:
{CATEGORY_MAPPING}
ห้ามใช้ชื่อ column ภาษาอังกฤษในคำตอบ ให้ใช้ชื่อภาษาไทยแทนเสมอ"""
            },
            {"role": "user", "content": f"""คำถาม: {question}

SQL ที่ใช้: {sql}

ผลลัพธ์: {results}

กรุณาสรุปคำตอบเป็นภาษาไทยให้เข้าใจง่าย"""}
        ]
    )
    return response.choices[0].message.content.strip()


def suggest_chart_type(question: str, df: pd.DataFrame) -> str:
    """แนะนำประเภท chart ที่เหมาะสม"""
    response = client.chat.completions.create(
        model=GPT_MODEL,
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


# ===== Main function =====

def fix_sql_with_error(sql: str, error_msg: str, question: str) -> str:
    """ให้ GPT แก้ SQL เมื่อ Hive error"""
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": f"""คุณเป็น HiveQL expert ช่วยแก้ SQL query ที่ error
{TABLE_SCHEMA}
ตอบเฉพาะ SQL query ที่แก้แล้วเท่านั้น ไม่ต้องอธิบาย"""
            },
            {"role": "user", "content": f"""คำถาม: {question}

SQL ที่ error:
{sql}

Error message:
{error_msg}

กรุณาแก้ SQL ให้ถูกต้อง"""}
        ]
    )
    fixed = response.choices[0].message.content.strip()
    if fixed.startswith("```"):
        lines = fixed.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        fixed = "\n".join(lines).strip()
    fixed = fixed.rstrip(";")
    return fix_hive_reserved_keywords(fixed)


def chat_with_data_full(question: str) -> dict:
    """ถามคำถามแล้ว return ผลลัพธ์ทั้งหมด (สำหรับ Dashboard)"""
    sql = ask_gpt_for_sql(question)

    # ถ้า GPT ใช้ SUM(CASE WHEN remaining) ให้บังคับ rewrite ทันที
    if has_bad_remaining_sum(sql):
        error_hint = "ข้อผิดพลาด: คุณใช้ SUM(CASE WHEN details='remaining') ซึ่งผิดหลักการ เพราะ remaining คือ running balance ไม่ใช่ยอดต่อเดือน กรุณาเขียน query ใหม่โดยใช้ JOIN กับ subquery MAX(`date`) แทน ห้ามใช้ SUM กับ remaining เด็ดขาด"
        sql = fix_sql_with_error(sql, error_hint, question)

    # retry ถ้า Hive error (สูงสุด 2 ครั้ง)
    df = None
    last_error = None
    for attempt in range(3):
        try:
            df = execute_query_df(sql)
            last_error = None
            break
        except Exception as e:
            last_error = str(e)
            if attempt < 2:
                sql = fix_sql_with_error(sql, last_error, question)
            else:
                raise Exception(f"ลอง {attempt+1} ครั้งแล้วยังไม่ได้:\n{last_error}")

    summary = ask_gpt_to_summarize(question, sql, df.to_string())
    chart_type = suggest_chart_type(question, df) if len(df) > 0 else "none"

    return {
        "question": question,
        "sql": sql,
        "df": df,
        "summary": summary,
        "chart_type": chart_type
    }