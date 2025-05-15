import streamlit as st

st.set_page_config(page_title="Welcome", layout="wide")

st.title("🗃️ Database Peeker")
st.markdown(
    """
    **How to use this app**

    -> Pick any table from the sidebar to load previews or data summary.  
    -> Adjust the row counts (1-20) and click **Load**.  
    -> Use **Reload** to refresh the cache.

    ---
    """,
    unsafe_allow_html=True,
)
st.info("Select a table from the sidebar to begin")

