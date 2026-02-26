# components/chat.py
import streamlit as st
import plotly.express as px
import pandas as pd
from utils.history import load_chat_history, add_to_history
from services.hive_gpt import chat_with_data_full


def render_chart(df: pd.DataFrame, chart_type: str, title: str):
    """Render chart ตาม chart_type"""
    if len(df.columns) < 2:
        return None

    chart = None
    if chart_type == "bar":
        chart = px.bar(df, x=df.columns[0], y=df.columns[1], title=title)
    elif chart_type == "line":
        chart = px.line(df, x=df.columns[0], y=df.columns[1], title=title)
    elif chart_type == "pie":
        chart = px.pie(df, names=df.columns[0], values=df.columns[1], title=title)

    if chart:
        st.plotly_chart(chart, use_container_width=True)

    return chart


def render_history_view():
    """แสดง chat จาก history ที่เลือก"""
    history = load_chat_history()
    chat_id = st.session_state.current_chat_id

    if not (0 <= chat_id < len(history)):
        return

    item = history[chat_id]
    st.info(f"📜 ดูประวัติ: {item['timestamp']}")

    with st.chat_message("user"):
        st.markdown(item['question'])

    with st.chat_message("assistant"):
        with st.expander("🔍 SQL Query", expanded=False):
            st.code(item['sql'], language="sql")

        if item.get('df_json'):
            try:
                df = pd.read_json(item['df_json'])
                if len(df) > 0:
                    st.dataframe(df, use_container_width=True)
                    render_chart(df, item.get('chart_type', 'none'), item['question'])
            except Exception:
                pass

        st.markdown(f"**💬 สรุป:** {item['summary']}")

    if st.button("➕ เริ่ม Chat ใหม่", use_container_width=True):
        st.session_state.current_chat_id = None
        st.session_state.messages = []
        st.rerun()


def process_question(question: str) -> dict:
    """เรียก GPT+Hive แล้วบันทึก history"""
    result = chat_with_data_full(question)
    df = result["df"]
    add_to_history(
        question=question,
        sql=result["sql"],
        summary=result["summary"],
        df_json=df.to_json() if len(df) > 0 else "",
        chart_type=result["chart_type"]
    )
    return result


def render_chat():
    """แสดง chat ปัจจุบัน + chat input"""

    # แสดง messages ที่มีอยู่
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant" and msg.get("sql"):
                with st.expander("🔍 SQL Query", expanded=False):
                    st.code(msg["sql"], language="sql")
            st.markdown(msg["content"])
            if "df" in msg and msg["df"] is not None and len(msg["df"]) > 0:
                st.dataframe(msg["df"], use_container_width=True)
            if "chart" in msg and msg["chart"] is not None:
                st.plotly_chart(msg["chart"], use_container_width=True)

    # Process pending question
    if st.session_state.pending_question:
        question = st.session_state.pending_question
        st.session_state.pending_question = None

        st.session_state.messages.append({"role": "user", "content": question})

        with st.chat_message("user"):
            st.markdown(question)

        # KEY FIX: spinner ต้องอยู่นอก chat_message และนอก expander
        result = None
        error = None
        with st.spinner("กำลังวิเคราะห์..."):
            try:
                result = process_question(question)
            except Exception as e:
                error = e

        # Render ผลลัพธ์หลัง spinner จบแล้ว
        with st.chat_message("assistant"):
            if error:
                st.error(f"เกิดข้อผิดพลาด: {error}")
            elif result:
                with st.expander("🔍 SQL Query", expanded=False):
                    st.code(result["sql"], language="sql")

                df = result["df"]
                chart = None

                if len(df) > 0:
                    st.dataframe(df, use_container_width=True)
                    chart = render_chart(df, result["chart_type"], question)

                st.markdown(f"**💬 สรุป:** {result['summary']}")

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"**💬 สรุป:** {result['summary']}",
                    "sql": result["sql"],
                    "df": df,
                    "chart": chart
                })

    # Chat input
    if question := st.chat_input("ถามคำถามเกี่ยวกับงบประมาณ..."):
        st.session_state.pending_question = question
        st.rerun()