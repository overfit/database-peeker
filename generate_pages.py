# generate_pages.py ---------------------------------------------------------
"""
Create/refresh Streamlit page stubs under ./pages/
One file per table (views included) found in the [dmd] schema.
"""
import os
import re
from pathlib import Path
from sqlalchemy import inspect
from connection_service import ConnectionService

SRC_DIR = Path(__file__).parent / "pages"
SRC_DIR.mkdir(exist_ok=True)

# 1️⃣  discover tables & views ------------------------------------------------
engine = ConnectionService().engine
insp   = inspect(engine)

def list_objects(schema="dmd"):
    for vw in insp.get_view_names(schema=schema):
        yield vw
    # uncomment to include tables in addition to views
    # for tbl in insp.get_table_names(schema=schema):
    #     yield tbl

objects = sorted(list_objects())  # e.g. ["V_BIInvCNLines", "V_Customers", …]

# 2️⃣  template --------------------------------------------------------------
STUB = """\
from common import render_table
render_table("{title}", "[dmd].[{name}]")
"""

# 3️⃣  write/update files ----------------------------------------------------
def to_filename(name: str) -> str:
    # safe_kebab.py → “v_biinvcnlines.py”
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_").lower()
    return f"{safe}.py"

for obj in objects:
    file_path = SRC_DIR / to_filename(obj)
    file_path.write_text(STUB.format(title=obj, name=obj), encoding="utf-8")
    print("✓", file_path.relative_to(Path.cwd()))

print("\nDone. Restart Streamlit to pick up the new pages.")
