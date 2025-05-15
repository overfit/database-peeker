import re
from pathlib import Path
import streamlit as st

PAGES_DIR = Path(__file__).parent / "pages"

def parse_title(file_path: Path):
    """
    Look for render_table("Some Title", ...  in the page stub;
    return 'Some Title' or None if not found.
    """
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r'render_table\(\s*["\'](.+?)["\']', text)
    return match.group(1) if match else None


def discover_pages() -> dict[str, list[st.Page]]:
    # Landing / home page (assumed to be pages/home.py)
    home_stub = PAGES_DIR / "home.py"
    home_group = [
        st.Page(str(home_stub), title="Home", icon="🏠")
    ] if home_stub.exists() else []

    # All generated table stubs
    table_pages = []
    for p in sorted(PAGES_DIR.glob("*.py")):
        if p.name.startswith("home"):        # skip the landing stub
            continue
        title = parse_title(p)
        table_pages.append(
            st.Page(str(p), title=f"[dmd].{title}", icon="📄")
        )

    # Two groups: Home first, tables second
    return {
        "": home_group,                 # blank header (or use "Home")
        "Tables and Views": table_pages
    }


pages = discover_pages()
pg = st.navigation(pages)
pg.run()
