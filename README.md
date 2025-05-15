# Database Peeker

A lightweight **Streamlit** app for browsing tables and views from the external database sources.

Features:

* On-demand fetch  
  * **TOP N rows** (quick preview)  
  * **random sample** & **summary stats**
* Rows to display (1 – 20)
* Data reload and cache cleaning  
* Auto-generate of Streamlit pages for all available views

## Really Quick Start

```
git clone <repo_url>
cd <repo>
pip install -r requirements.txt
nano .env  # follow .env.example
python generate_pages.py
streamlit run app.py
```

🚧 Work in progress 🚧
