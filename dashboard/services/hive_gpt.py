# services/hive_gpt.py
import os
import re
import pandas as pd
import streamlit as st
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
    return hive.Connection(host=HIVE_HOST, port=HIVE_PORT, database=HIVE_DATABASE)


def execute_query(sql: str) -> list:
    conn = get_hive_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.close()
    return result


def execute_query_df(sql: str) -> pd.DataFrame:
    conn = get_hive_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data, columns=columns)


# ===== SQL Utils =====

def fix_hive_reserved_keywords(sql: str) -> str:
    sql = re.sub(r'(?<!`)\bdate\b(?!`)', '`date`', sql)
    return sql


def has_bad_remaining_sum(sql: str) -> bool:
    pattern = r'SUM\s*\(\s*CASE\s+WHEN\s+\S*details\S*\s*=\s*[\'"`]remaining[\'"`]'
    return bool(re.search(pattern, sql, re.IGNORECASE))


def _clean_sql(sql: str) -> str:
    """ทำความสะอาด SQL จาก GPT output"""
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        sql = "\n".join(lines).strip()
    sql = sql.replace("\u2018", "'").replace("\u2019", "'")
    sql = sql.replace("\u201c", '"').replace("\u201d", '"')
    sql = sql.rstrip(";")
    return fix_hive_reserved_keywords(sql)


def _get_year_context() -> str:
    """ดึง year context จาก session_state สำหรับใส่ใน prompt"""
    selected_year = st.session_state.get("selected_year", 2024)
    return f"""ปีงบประมาณที่ user เลือกอยู่ตอนนี้: {selected_year}
- ถ้าคำถามไม่ระบุปี ให้ใช้ year = {selected_year} เสมอ
- ถ้าคำถามถามเปรียบเทียบหลายปี เช่น "ปี 2022 vs 2023" ให้ใช้ WHERE year IN (2022, 2023)"""


# ===== GPT =====

def ask_gpt_for_sql(user_question: str) -> str:
    year_context = _get_year_context()
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": f"""คุณเป็น SQL expert สำหรับ Hive/Hadoop
ให้แปลงคำถามภาษาไทยเป็น HiveQL query

{TABLE_SCHEMA}

{year_context}

ข้อห้ามเด็ดขาด:
- ห้าม SUM(amount) WHERE details = 'remaining' ข้ามหลายเดือน
- หากคำถามถามเรื่อง "ยอดคงเหลือ" หรือ "remaining" ให้ดึงเฉพาะเดือนล่าสุดเสมอ

ตอบเฉพาะ SQL query เท่านั้น ไม่ต้องอธิบาย ไม่ต้องใส่ markdown code block"""
            },
            {"role": "user", "content": user_question}
        ]
    )
    return _clean_sql(response.choices[0].message.content.strip())


def ask_gpt_to_summarize(question: str, sql: str, results) -> str:
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
            {"role": "user", "content": f"คำถาม: {question}\nSQL: {sql}\nผลลัพธ์: {results}\nกรุณาสรุปเป็นภาษาไทย"}
        ]
    )
    return response.choices[0].message.content.strip()


def suggest_chart_type(question: str, df: pd.DataFrame) -> str:
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": "วิเคราะห์คำถามและข้อมูล แล้วแนะนำประเภท chart ที่เหมาะสม\nตอบเพียงคำเดียว: bar, line, pie, none"
            },
            {"role": "user", "content": f"คำถาม: {question}\nColumns: {df.columns.tolist()}\nRows: {len(df)}"}
        ]
    )
    return response.choices[0].message.content.strip().lower()


def fix_sql_with_error(sql: str, error_msg: str, question: str) -> str:
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": f"""คุณเป็น HiveQL expert ช่วยแก้ SQL query ที่ error
{TABLE_SCHEMA}
ตอบเฉพาะ SQL query ที่แก้แล้วเท่านั้น"""
            },
            {"role": "user", "content": f"คำถาม: {question}\nSQL:\n{sql}\nError:\n{error_msg}\nกรุณาแก้ SQL"}
        ]
    )
    return _clean_sql(response.choices[0].message.content.strip())


def chat_with_data_full(question: str) -> dict:
    sql = ask_gpt_for_sql(question)

    if has_bad_remaining_sum(sql):
        error_hint = "ข้อผิดพลาด: ห้ามใช้ SUM(CASE WHEN details='remaining') ให้ใช้ JOIN กับ subquery MAX(`date`) แทน"
        sql = fix_sql_with_error(sql, error_hint, question)

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