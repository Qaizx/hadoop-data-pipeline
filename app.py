# app.py
import streamlit as st
import plotly.express as px
from gpt_connect_finance_itsc import chat_with_data_full, execute_query_df
from datetime import datetime
import json
import os
import pandas as pd

# ===== Config =====
st.set_page_config(
    page_title="Finance ITSC Dashboard",
    page_icon="💰",
    layout="wide"
)

HISTORY_FILE = "chat_history.json"

# ===== Functions =====
@st.cache_data(ttl=1)  # Cache 1 วินาที เพื่อให้โหลดใหม่เร็ว
def load_chat_history():
    """โหลด chat history จากไฟล์"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_chat_history(history):
    """บันทึก chat history ลงไฟล์"""
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    # Clear cache เพื่อให้โหลดใหม่
    load_chat_history.clear()

def add_to_history(question: str, sql: str, summary: str, df_json: str, chart_type: str):
    """เพิ่มคำถามลง history"""
    history = load_chat_history()
    history.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "question": question,
        "sql": sql,
        "summary": summary,
        "df_json": df_json,
        "chart_type": chart_type
    })
    history = history[-50:]
    save_chat_history(history)

def process_question(question: str):
    """ประมวลผลคำถามและบันทึก history"""
    result = chat_with_data_full(question)
    
    df = result["df"]
    
    # Save to history
    add_to_history(
        question=question,
        sql=result["sql"],
        summary=result['summary'],
        df_json=df.to_json() if len(df) > 0 else "",
        chart_type=result["chart_type"]
    )
    
    return result

# ===== Initialize Session State =====
if "messages" not in st.session_state:
    st.session_state.messages = []

if "current_chat_id" not in st.session_state:
    st.session_state.current_chat_id = None

if "pending_question" not in st.session_state:
    st.session_state.pending_question = None

# ===== Sidebar =====
with st.sidebar:
    st.header("📊 Quick Stats")
    
    try:
        df_budget = execute_query_df("""
            SELECT SUM(amount) as total 
            FROM finance_itsc_long 
            WHERE details = 'budget' AND year = 2024
        """)
        total_budget = df_budget['total'].iloc[0] or 0
        
        df_spent = execute_query_df("""
            SELECT SUM(amount) as total 
            FROM finance_itsc_long 
            WHERE details = 'spent' AND year = 2024
        """)
        total_spent = df_spent['total'].iloc[0] or 0
        
        remaining = total_budget - total_spent
        percent_used = (total_spent / total_budget * 100) if total_budget > 0 else 0
        
        st.metric("งบประมาณทั้งหมด", f"{total_budget:,.0f} ฿")
        st.metric("ใช้ไปแล้ว", f"{total_spent:,.0f} ฿", f"{percent_used:.1f}%")
        st.metric("คงเหลือ", f"{remaining:,.0f} ฿")
        
        st.progress(min(percent_used / 100, 1.0))
        
    except Exception as e:
        st.error(f"โหลดข้อมูลไม่ได้: {e}")
    
    st.markdown("---")
    
    st.header("📜 ประวัติการถาม")
    
    history = load_chat_history()
    
    if history:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ ล้างประวัติ", use_container_width=True):
                save_chat_history([])
                st.session_state.messages = []
                st.session_state.current_chat_id = None
                st.rerun()
        with col2:
            if st.button("➕ Chat ใหม่", use_container_width=True):
                st.session_state.messages = []
                st.session_state.current_chat_id = None
                st.rerun()
        
        for i, item in enumerate(reversed(history)):
            idx = len(history) - 1 - i
            short_question = item['question'][:25] + "..." if len(item['question']) > 25 else item['question']
            
            if st.button(f"🕐 {item['timestamp']}\n{short_question}", key=f"history_{i}", use_container_width=True):
                st.session_state.current_chat_id = idx
                st.session_state.messages = []
                st.rerun()
    else:
        st.info("ยังไม่มีประวัติการถาม")

# ===== Main Content =====
st.title("💰 Finance ITSC Dashboard")
st.markdown("ถามคำถามเกี่ยวกับงบประมาณเป็นภาษาไทยได้เลย")

# ===== แสดงประวัติ chat ที่เลือก =====
if st.session_state.current_chat_id is not None:
    history = load_chat_history()
    if 0 <= st.session_state.current_chat_id < len(history):
        item = history[st.session_state.current_chat_id]
        
        st.info(f"📜 ดูประวัติ: {item['timestamp']}")
        
        with st.chat_message("user"):
            st.markdown(item['question'])
        
        with st.chat_message("assistant"):
            with st.expander("🔍 SQL Query"):
                st.code(item['sql'], language="sql")
            
            if item.get('df_json'):
                try:
                    df = pd.read_json(item['df_json'])
                    if len(df) > 0:
                        st.dataframe(df, use_container_width=True)
                        
                        chart_type = item.get('chart_type', 'none')
                        if chart_type == "bar" and len(df.columns) >= 2:
                            chart = px.bar(df, x=df.columns[0], y=df.columns[1], title=item['question'])
                            st.plotly_chart(chart, use_container_width=True)
                        elif chart_type == "line" and len(df.columns) >= 2:
                            chart = px.line(df, x=df.columns[0], y=df.columns[1], title=item['question'])
                            st.plotly_chart(chart, use_container_width=True)
                        elif chart_type == "pie" and len(df.columns) >= 2:
                            chart = px.pie(df, names=df.columns[0], values=df.columns[1], title=item['question'])
                            st.plotly_chart(chart, use_container_width=True)
                except:
                    pass
            
            st.markdown(f"**💬 สรุป:** {item['summary']}")
        
        if st.button("➕ เริ่ม Chat ใหม่", use_container_width=True):
            st.session_state.current_chat_id = None
            st.session_state.messages = []
            st.rerun()

# ===== แสดง Chat ปัจจุบัน =====
else:
    # Display messages
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "df" in msg and msg["df"] is not None and len(msg["df"]) > 0:
                st.dataframe(msg["df"], use_container_width=True)
            if "chart" in msg and msg["chart"] is not None:
                st.plotly_chart(msg["chart"], use_container_width=True)

    # ===== Process pending question =====
    if st.session_state.pending_question:
        question = st.session_state.pending_question
        st.session_state.pending_question = None
        
        st.session_state.messages.append({"role": "user", "content": question})
        
        with st.chat_message("user"):
            st.markdown(question)
        
        with st.chat_message("assistant"):
            with st.spinner("กำลังวิเคราะห์..."):
                try:
                    result = process_question(question)
                    
                    with st.expander("🔍 SQL Query"):
                        st.code(result["sql"], language="sql")
                    
                    df = result["df"]
                    chart = None
                    
                    if len(df) > 0:
                        st.dataframe(df, use_container_width=True)
                        
                        if result["chart_type"] == "bar" and len(df.columns) >= 2:
                            chart = px.bar(df, x=df.columns[0], y=df.columns[1], title=question)
                            st.plotly_chart(chart, use_container_width=True)
                        elif result["chart_type"] == "line" and len(df.columns) >= 2:
                            chart = px.line(df, x=df.columns[0], y=df.columns[1], title=question)
                            st.plotly_chart(chart, use_container_width=True)
                        elif result["chart_type"] == "pie" and len(df.columns) >= 2:
                            chart = px.pie(df, names=df.columns[0], values=df.columns[1], title=question)
                            st.plotly_chart(chart, use_container_width=True)
                    
                    st.markdown(f"**💬 สรุป:** {result['summary']}")
                    
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"**💬 สรุป:** {result['summary']}",
                        "df": df,
                        "chart": chart
                    })
                    
                    # Rerun เพื่ออัพเดท sidebar
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"เกิดข้อผิดพลาด: {e}")

    # ===== Chat Input =====
    if question := st.chat_input("ถามคำถามเกี่ยวกับงบประมาณ..."):
        st.session_state.pending_question = question
        st.rerun()

    # # ===== Quick Questions =====
    # st.header("❓ คำถามยอดนิยม")

    # col1, col2, col3 = st.columns(3)

    # with col1:
    #     if st.button("💵 งบปี 2024 ใช้ไปเท่าไร?", use_container_width=True):
    #         st.session_state.pending_question = "งบปี 2024 ใช้ไปเท่าไร?"
    #         st.rerun()

    # with col2:
    #     if st.button("📊 หมวดไหนใช้งบเยอะสุด?", use_container_width=True):
    #         st.session_state.pending_question = "หมวดไหนใช้งบเยอะที่สุดในปี 2024"
    #         st.rerun()

    # with col3:
    #     if st.button("📈 เปรียบเทียบรายเดือน", use_container_width=True):
    #         st.session_state.pending_question = "เปรียบเทียบค่าใช้จ่ายแต่ละเดือนในปี 2024"
    #         st.rerun()