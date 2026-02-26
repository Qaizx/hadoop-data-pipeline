# components/sidebar.py
import streamlit as st
from auth import logout
from utils.history import load_chat_history, clear_history
from services.hive_gpt import execute_query_df


@st.cache_data(ttl=300)  # cache 5 นาที ไม่ต้องยิง Hive ทุก rerun
def load_quick_stats():
    """ดึงข้อมูล Quick Stats จาก Hive"""
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

    return total_budget, total_spent


def render_sidebar():
    with st.sidebar:
        _render_user_section()
        st.markdown("---")
        _render_quick_stats()
        st.markdown("---")
        _render_history_section()


def _render_user_section():
    st.markdown(f"👤 **{st.session_state.get('username', '')}**")
    if st.button("🚪 ออกจากระบบ", use_container_width=True):
        logout()


def _render_quick_stats():
    st.header("📊 Quick Stats")
    try:
        total_budget, total_spent = load_quick_stats()
        remaining = total_budget - total_spent
        percent_used = (total_spent / total_budget * 100) if total_budget > 0 else 0

        st.metric("งบประมาณทั้งหมด", f"{total_budget:,.0f} ฿")
        st.metric("ใช้ไปแล้ว", f"{total_spent:,.0f} ฿", f"{percent_used:.1f}%")
        st.metric("คงเหลือ", f"{remaining:,.0f} ฿")
        st.progress(min(percent_used / 100, 1.0))

    except Exception as e:
        st.error(f"โหลดข้อมูลไม่ได้: {e}")


def _render_history_section():
    st.header("📜 ประวัติการถาม")
    history = load_chat_history()

    if not history:
        st.info("ยังไม่มีประวัติการถาม")
        return

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ ล้างประวัติ", use_container_width=True):
            clear_history()
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
        short_q = item['question'][:25] + "..." if len(item['question']) > 25 else item['question']
        if st.button(f"🕐 {item['timestamp']}\n{short_q}", key=f"history_{i}", use_container_width=True):
            st.session_state.current_chat_id = idx
            st.session_state.messages = []
            st.rerun()