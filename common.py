# common.py -----------------------------------------------------------------
import logging
import pandas as pd
import streamlit as st
from sqlalchemy import text
from connection_service import ConnectionService

# ─────────────────────────  logging  ───────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────  db engine  ─────────────────────────────────────
connection = ConnectionService()
engine = connection.engine

# ─────────────────────────  helpers  ───────────────────────────────────────
@st.cache_resource(show_spinner=False)
def load_full(sql_name: str) -> pd.DataFrame:
    log.info("Loading FULL table %s …", sql_name)
    return pd.read_sql(text(f"SELECT * FROM {sql_name}"), engine)

@st.cache_resource(show_spinner=False)
def load_top(sql_name: str, n: int) -> pd.DataFrame:
    log.info("Loading TOP %s rows from %s …", n, sql_name)
    return pd.read_sql(text(f"SELECT TOP {n} * FROM {sql_name}"), engine)

# ─────────────────────────  page renderer  ─────────────────────────────────
def render_table(label: str, sql_name: str) -> None:
    st.set_page_config(page_title=label, layout="wide")

    # session-state keys
    top_key      = f"{label}_TOP"
    topn_key     = f"{label}_TOP_N"
    full_key     = f"{label}_FULL"
    samplen_key  = f"{label}_SAMPLE_N"

    st.title(label)

    # ── choose N for each subset (1-20) ────────────────────────────────────
    col_n1, col_n2 = st.columns(2, gap="medium")
    with col_n1:
        sample_n = st.number_input(
            "Rows for **Random sample** (1 – 20)",
            min_value=1,
            max_value=20,
            value=st.session_state.get(samplen_key, 5),
            step=1,
            key=f"{label}-sample-n",
        )
        st.session_state[samplen_key] = sample_n

    with col_n2:
        top_n = st.number_input(
            "Rows for **TOP-N** view (1 – 20)",
            min_value=1,
            max_value=20,
            value=st.session_state.get(topn_key, 5),
            step=1,
            key=f"{label}-top-n",
        )
        st.session_state[topn_key] = top_n

    # st.divider()

    # ── button row ─────────────────────────────────────────────────────────
    col1, col2 = st.columns(2, gap="large")

    # ---- FULL loader (summary & random) -----------------------------------
    with col1:
        if full_key not in st.session_state:
            if st.button("🔄 Load summary & random sample", key=f"{label}-full-load"):
                with st.spinner("Fetching entire table…"):
                    st.session_state[full_key] = load_full(sql_name)
                st.rerun()
        else:
            if st.button("↻ Reload summary/sample", key=f"{label}-full-reload"):
                with st.spinner("Reloading full table…"):
                    st.session_state[full_key] = load_full(sql_name)
                st.rerun()

    # ---- TOP-N loader -----------------------------------------------------
    with col2:
        if top_key not in st.session_state:
            if st.button("👀 Load TOP-N rows", key=f"{label}-top-load"):
                with st.spinner("Fetching top rows…"):
                    st.session_state[top_key] = load_top(sql_name, top_n)
                st.rerun()
        else:
            if st.button("↻ Reload TOP-N", key=f"{label}-top-reload"):
                with st.spinner("Reloading top rows…"):
                    st.session_state[top_key] = load_top(sql_name, top_n)
                st.rerun()

    st.divider()

    # ── render TOP-N if present ────────────────────────────────────────────
    if top_key in st.session_state:
        st.subheader(f"📋 TOP {top_n} rows")
        st.dataframe(
            st.session_state[top_key].astype(str),
            use_container_width=True,
            height=250,
        )
        st.divider()

    # ── render summary & random sample if present ──────────────────────────
    if full_key in st.session_state:
        df = st.session_state[full_key]

        st.subheader(f"🎲 Random sample of {sample_n} rows")
        st.dataframe(
            df.sample(n=min(sample_n, len(df)), random_state=42).astype(str),
            use_container_width=True,
            height=250,
        )

        st.subheader("📊 Summary statistics")
        summary = df.describe(include="all").T
        st.dataframe(summary.astype(str), use_container_width=True, height=400)
