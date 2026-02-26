# auth.py
import streamlit as st


def check_login(username: str, password: str) -> bool:
    users = st.secrets.get("users", {})
    return users.get(username) == password


def login_page():
    """แสดงหน้า Login แบบ fullscreen ไม่มี sidebar"""
    # ซ่อน sidebar ด้วย CSS
    st.markdown("""
        <style>
            [data-testid="stSidebar"] { display: none; }
            [data-testid="collapsedControl"] { display: none; }
        </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.title("💰 Finance ITSC Dashboard")
        st.markdown("---")
        st.subheader("🔐 เข้าสู่ระบบ")

        with st.form("login_form"):
            username = st.text_input("ชื่อผู้ใช้", placeholder="username")
            password = st.text_input("รหัสผ่าน", type="password", placeholder="password")
            submitted = st.form_submit_button("เข้าสู่ระบบ", use_container_width=True)

            if submitted:
                if check_login(username, password):
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.rerun()
                else:
                    st.error("ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")


def require_auth():
    """ถ้ายังไม่ login ให้แสดงหน้า login แล้วหยุด"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        login_page()
        st.stop()


def logout():
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.messages = []
    st.session_state.current_chat_id = None
    st.rerun()