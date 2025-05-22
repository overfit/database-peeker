import logging
import pandas as pd
import streamlit as st
from sqlalchemy import text, inspect
from sqlalchemy.sql import sqltypes
from connection_service import ConnectionService
import numpy as np
import datetime
import decimal
import re

# ─────────────────────────  logging  ───────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────  db engine / inspector  ─────────────────────────
connection = ConnectionService()
engine = connection.engine
insp = inspect(engine)

# ─────────────────────────  helpers  ───────────────────────────────────────

def display_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy where all pandas-missing values (NaN / None / NaT) are
    replaced by the literal string '<NULL>'.
    This makes real SQL NULLs visually distinct from the text 'None'.
    """
    # Work on object dtype so we don't cast numerics to strings *in the original df*
    out = df.copy().astype("object")
    mask = out.isna()
    out[mask] = "<NULL>"
    return out

def split_qualified(name: str):
    """
    "[dmd].[V_BIInvCNLines]" → ("dmd", "V_BIInvCNLines")
    "dbo.Customers"          → ("dbo", "Customers")
    "Orders"                 → (None, "Orders")
    """
    # remove [...], then split on dot
    parts = re.sub(r"[\[\]]", "", name).split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]

@st.cache_resource(show_spinner=False)
def get_table_counts(sql_name: str):
    """Return (n_cols, n_rows) for the given table/view."""
    schema, table = split_qualified(sql_name)
    # column count via inspector
    n_cols = len(insp.get_columns(table, schema=schema))
    # row count via lightweight SQL
    n_rows = pd.read_sql(text(f"SELECT COUNT(*) AS n FROM {sql_name}"), engine)["n"][0]
    return n_cols, n_rows


@st.cache_resource(show_spinner=False)
def load_top(sql_name: str, n: int) -> pd.DataFrame:
    log.info("Loading TOP %s rows from %s …", n, sql_name)
    q = f"SELECT TOP {n} * FROM {sql_name}"
    return pd.read_sql(text(q), engine)

@st.cache_resource(show_spinner=False)
def load_random(sql_name: str, n: int) -> pd.DataFrame:
    log.info("Loading RANDOM %s rows from %s …", n, sql_name)
    q = f"SELECT TOP {n} * FROM {sql_name} ORDER BY NEWID()"
    return pd.read_sql(text(q), engine)

@st.cache_resource(show_spinner=False)
def load_column_summary(sql_name: str) -> pd.DataFrame:
    """
    One query → min / max / avg (numeric, datetime), distinct-count (where allowed),
    null-count (all). Columns that can't use DISTINCT get NULL in the unique field.
    """
    schema, table = split_qualified(sql_name)
    cols_meta = insp.get_columns(table, schema=schema)

    # ── which SQLAlchemy types fail in COUNT(DISTINCT …) on SQL Server? ──
    UNSUPPORTED_DISTINCT = (
        sqltypes.LargeBinary,
        sqltypes.Text,
        sqltypes.UnicodeText,
        sqltypes.VARBINARY,
        sqltypes.JSON,            # in case of sqltypes.JSON
        sqltypes.NullType,        # unknown mapping
    )

    agg_fragments = []
    all_ptype = {}
    for col in cols_meta:
        n      = col["name"]
        stype  = col["type"]
        ptype  = None
        try:                      # some drivers raise on python_type
            ptype = stype.python_type
            all_ptype[n] = ptype
        except NotImplementedError:
            pass

        # ---- bool -------------------------------------------------------
        if ptype is bool:
            min_expr = "NULL"
            max_expr = "NULL"
            avg_expr = "NULL"

        # ---- numeric ----------------------------------------------------
        elif ptype and issubclass(ptype, (int, float, decimal.Decimal)):
            min_expr = f"MIN([{n}])"
            max_expr = f"MAX([{n}])"
            avg_expr = f"AVG([{n}])"

        # ---- datetime and string -----------------------------------------
        elif ptype and issubclass(ptype, (datetime.date, datetime.datetime, str)):
            min_expr = f"MIN([{n}])"
            max_expr = f"MAX([{n}])"
            avg_expr = "NULL"

        # ---- everything else -------------------------------------------
        else:
            min_expr = max_expr = avg_expr = "NULL"

        # ---- distinct-count support? ------------------------------------
        if isinstance(stype, UNSUPPORTED_DISTINCT):
            uniq_expr = "NULL"
        else:
            uniq_expr = f"COUNT(DISTINCT [{n}])"

        # ---- null-count --------------------------------------------------
        null_expr = f"SUM(CASE WHEN [{n}] IS NULL THEN 1 ELSE 0 END)"

        agg_fragments.extend([
            f"{min_expr}  AS [{n}_min]",
            f"{max_expr}  AS [{n}_max]",
            f"{avg_expr}  AS [{n}_avg]",
            f"{uniq_expr} AS [{n}_unique]",
            f"{null_expr} AS [{n}_nulls]",
        ])
    # print(all_ptype)
    query = f"SELECT {', '.join(agg_fragments)} FROM {sql_name}"
    row   = pd.read_sql(text(query), engine).iloc[0]

    # reshape
    records = []
    for col in cols_meta:
        n = col["name"]
        stype = col["type"]
        dtype = getattr(stype, "__visit_name__", stype.__class__.__name__).upper()
        
        try:
            ptype = stype.python_type.__name__
        except (NotImplementedError, AttributeError):
            ptype = "<NULL>"

        records.append(
            dict(
                column=n,
                sqltype = dtype,
                pytype = ptype,
                min=row[f"{n}_min"],
                max=row[f"{n}_max"],
                avg=row[f"{n}_avg"],
                unique=row[f"{n}_unique"],
                nulls=row[f"{n}_nulls"],
            )
        )
    return pd.DataFrame(records)


@st.cache_resource(show_spinner=False)
def load_unique_counts(sql_name: str, col: str, limit: int = 300):
    """
    Return a DataFrame of unique values + counts, or None when distinct > limit.
    BIT columns are mapped to 'True' / 'False'; everything else stays raw.
    """
    # ---------- 1. run the generic SQL -----------------------------------
    q = f"""
        SELECT CAST([{col}] AS NVARCHAR(MAX)) AS value, COUNT(*) AS count
        FROM {sql_name}
        GROUP BY [{col}]
        ORDER BY count DESC
    """
    df = pd.read_sql(text(q), engine)

    # ---------- 2. detect real column type via inspector -----------------
    schema, table = split_qualified(sql_name)
    col_meta = next(
        c for c in insp.get_columns(table, schema=schema)
        if c["name"] == col
    )
    is_bit = (getattr(col_meta["type"], "__visit_name__", "").lower() == "bit")

    # ---------- 3. map only when BIT -------------------------------------
    if is_bit:
        df["value"] = df["value"].map({"1": "True", "0": "False", None: "<NULL>"})

    return df if len(df) <= limit else None

# ─────────────────────────  page renderer  ─────────────────────────────────
def render_table(label: str, sql_name: str) -> None:
    st.set_page_config(page_title=label, layout="wide")

    # session keys
    top_key      = f"{label}_TOP"
    topn_key     = f"{label}_TOP_N"
    rnd_key      = f"{label}_RANDOM"
    rndn_key     = f"{label}_RANDOM_N"
    summary_key  = f"{label}_SUMMARY"
    uniq_key     = f"{label}_UNIQUE_COUNTS"
    colsel_key   = f"{label}_COL_SELECTED"

    st.title(label)

    # ── pick row caps (1-20) ───────────────────────────────────────────────
    colA, colB = st.columns(2)
    with colA:
        rnd_n = st.number_input(
            "Rows for **Random sample** (1–20)",
            1, 20, st.session_state.get(rndn_key, 5), 1, key=f"{label}-rnd-n")
        st.session_state[rndn_key] = rnd_n
    with colB:
        top_n = st.number_input(
            "Rows for **TOP-N preview** (1–20)",
            1, 20, st.session_state.get(topn_key, 5), 1, key=f"{label}-top-n")
        st.session_state[topn_key] = top_n

    # st.divider()

    # ── preview buttons ────────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        if st.button("👀 Load TOP-N", key=f"{label}-top-load"):
            st.session_state[top_key] = load_top(sql_name, top_n)
            st.success("TOP-N loaded")
            st.rerun()

    with col2:
        if st.button("🎲 Load random sample & summary", key=f"{label}-rnd-load"):
            with st.spinner("Querying…"):
                st.session_state[rnd_key]     = load_random(sql_name, rnd_n)
                st.session_state[summary_key] = load_column_summary(sql_name)
            st.success("Random sample & summary loaded")
            st.rerun()
    
    st.divider()

    # --- counts ----------------------------------------------------------
    n_cols, n_rows = get_table_counts(sql_name)
    st.markdown(
        f"**Number of columns:** {n_cols}"
    )
    st.markdown(
        f"**Number of rows:** {n_rows:,}"
    )
    st.divider()

    # ── display TOP-N preview ─────────────────────────────────────────────
    if top_key in st.session_state:
        st.subheader(f"👀 TOP {top_n} rows")
        st.dataframe(display_df(st.session_state[top_key]).astype(str),
                     use_container_width=True, height=250)
        # st.divider()

    # ── display random sample & summary ────────────────────────────────────
    if rnd_key in st.session_state:
        st.subheader(f"🎲 Random {rnd_n} rows")
        st.dataframe(display_df(st.session_state[rnd_key]).astype(str),
                     use_container_width=True, height=250)

    if summary_key in st.session_state and not st.session_state[summary_key].empty:
        st.subheader("📊 Column summary")
        st.dataframe(
            display_df(st.session_state[summary_key]).astype(str),
            use_container_width=True,
            height=400,
        )


    # ── unique-value explorer (works without full table) ───────────────────
    # st.divider()
    st.subheader("🔍 Unique value counts (max 300 distinct)")

    # Need column list → get from inspector once
    schema, table = split_qualified(sql_name)
    columns = [c["name"] for c in insp.get_columns(table, schema=schema)]
    col_selected = st.selectbox(
        "Select column",
        options=columns,
        index=0 if colsel_key not in st.session_state else
               columns.index(st.session_state[colsel_key]),
        key=f"{label}-col-select"
    )
    st.session_state[colsel_key] = col_selected

    if st.button("🔍 Load unique counts", key=f"{label}-uniq"):
        with st.spinner("Counting…"):
            counts = load_unique_counts(sql_name, col_selected)
        if counts is None:
            st.warning("More than 300 unique values – table not shown.")
            st.session_state.pop(uniq_key, None)
        else:
            st.session_state[uniq_key] = counts
            st.success(f"{len(counts)} unique values loaded")
            st.rerun()

    if uniq_key in st.session_state:
        st.dataframe(
            display_df(st.session_state[uniq_key]).astype(str),
            use_container_width=True,
            height=min(400, 35 * len(st.session_state[uniq_key]) + 35),
        )
