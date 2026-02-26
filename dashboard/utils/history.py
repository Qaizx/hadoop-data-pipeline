# utils/history.py
import json
import os
import streamlit as st
from datetime import datetime
from config import HISTORY_FILE, MAX_HISTORY


@st.cache_data(ttl=1)
def load_chat_history() -> list:
    """โหลด chat history จากไฟล์"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_chat_history(history: list):
    """บันทึก chat history ลงไฟล์"""
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    load_chat_history.clear()


def add_to_history(question: str, sql: str, summary: str, df_json: str, chart_type: str):
    """เพิ่มรายการใหม่ลง history (จำกัดไว้ MAX_HISTORY รายการ)"""
    history = load_chat_history()
    history.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "question": question,
        "sql": sql,
        "summary": summary,
        "df_json": df_json,
        "chart_type": chart_type
    })
    history = history[-MAX_HISTORY:]
    save_chat_history(history)


def clear_history():
    """ล้าง history ทั้งหมด"""
    save_chat_history([])