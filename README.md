# 📊 Socio-Economic Report Dashboard with Chatbot Q\&A

This project is a **data dashboard system** for managing and visualizing socio-economic (KTXH) reports stored in PostgreSQL. It includes:

* 📈 Auto-generated charts from data tables
* 🗂️ Metadata management for tables and fields
* 🤖 A chatbot assistant that can answer questions about the database and charts using OpenAI or similar models
* ⚡ Real-time interactivity using Streamlit

---

## 🚀 Features

* **Chart Generation**: Automatically generate and render charts based on report data and `ind_code`
* **Metadata System**: Track descriptions of all tables and fields in `catalog.table_origin` and `catalog.field_origin`
* **Dashboard View**: Display multiple charts with title, description, and filters (e.g., `prd_id`, `org_id`)
* **Chatbot Integration**: Ask natural language questions about:

  * Dataset content
  * Chart meaning
  * SQL queries
* **Streamlit UI**: User-friendly web interface for interactive data exploration and Q\&A


---

## 📃 Tech Stack

* **Backend**: Python, Pandas, SQLAlchemy
* **Database**: PostgreSQL 17
* **Dashboard UI**: [Streamlit](https://streamlit.io/)
* **Charting**: [Plotly](https://plotly.com/python/)
* **Chatbot**: [Gemini](https://platform.openai.com/) (optional)

---

## 🧰 Sample Tables

* `bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753`: main data table for charting
* `catalog.table_origin`: stores metadata about tables
* `catalog.field_origin`: stores metadata about columns
* `rp_report`, `rp_input_grant`, `rp_period`, `sys_organization`: supporting metadata

---

## ⚙️ Installation

### 1. Clone & Install

```bash
git clone https://github.com/your-org/vdt-miniproject.git
cd vdt-miniproject
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure PostgreSQL

Make sure your PostgreSQL is running with required tables and metadata loaded.

Update variables in .env files:

```bash
copy sample.env .env
```

---

## 🧱 Usage

### ➔ Generate Metadata

```bash
python3 -m src.catalog.main
```

### ➔ Generate Dashboard and Charts

```bash
python3 -m src.catalog.gen_dash
```

### ➔ Launch Dashboard with Chatbot

```bash
streamlit run src/dashboard.py
```

---

## 💡 Future Plans

* ✅ Add support for different chart types (pie, area)
* ⌛ Add export/download chart feature
* 🔍 Use embedding + vector database to make chatbot more accurate
* 🔒 Add user authentication for dashboard

---

## 🤖 Chatbot Prompts (Examples)

> "bảng nào báo cáo chỉ tiêu tháng?"

> "Bảng bao cao chi tieu thang 7753 có các cột nào"

> "Cột tt4 mang ý nghĩa gì?"

---

## 📬 Contact

Developed by TrangNT – 2025\
📧 Email: [trangnt1317@gmail.com](mailto:trangnt1317@gmail.com)
🔗 [GitHub Profile](https://github.com/ntrg1317)

---