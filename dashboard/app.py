import streamlit as st
from auth import require_auth
from components.sidebar import render_sidebar
from components.chat import render_chat, render_history_view

# ===== Page Config =====
st.set_page_config(
    page_title="Finance ITSC Dashboard",
    page_icon="💰",
    layout="wide"
)

# ===== Auth Guard =====
require_auth()

# ===== Session State =====
if "messages" not in st.session_state:
    st.session_state.messages = []
if "current_chat_id" not in st.session_state:
    st.session_state.current_chat_id = None
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None

# ===== Layout =====
render_sidebar()

st.title("💰 Finance ITSC Dashboard")
st.markdown("ถามคำถามเกี่ยวกับงบประมาณเป็นภาษาไทยได้เลย")

if st.session_state.current_chat_id is not None:
    render_history_view()
else:
    render_chat()