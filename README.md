## 📦 E-Commerce Analytics Data Engineering Project

A complete end-to-end Data Engineering project focused on e-commerce sales analytics, from raw data ingestion to insights via dashboards.

> Built using: Python, MySQL (XAMPP), Pandas, SQLAlchemy, Power BI

---

### 📁 Project Structure

```
data-engineering-ecommerce-analytics-project/
│
├── data-engineering-ecommerce-analytics/
│   ├── data_raw/                # Raw CSVs (from Kaggle or other sources)
│   ├── data_processed/          # Cleaned/intermediate datasets
│   ├── scripts_ingestion/       # Scripts to load staging tables
│   ├── scripts_transform/       # Scripts to transform into star schema
│   ├── sql/                     # MySQL DDL & query scripts
│   ├── docker/                  # Docker setup (if containerized)
│   ├── airflow/                 # Airflow DAGs (future orchestration)
│   ├── powerBi/                 # Power BI workbook (.twbx)
│   └── docs/                    # ERD, architecture diagrams
└── README.md
```

---

## 🎯 Objective

To design and implement a robust data pipeline for e-commerce sales, transforming raw sales data into a structured star schema and visualizing KPIs using Tableau.

---

### ✅ Steps Completed

| Phase                         | Status      | Description                          |
| ----------------------------- | ----------- | ------------------------------------ |
| Data Exploration & Cleaning   | ✅ Done      | Cleaned Amazon order & sales data    |
| Database Schema (ERD)         | ✅ Done      | Designed staging + dimensional model |
| Build Data Pipeline           | ✅ Done      | Python + Pandas + SQLAlchemy         |
| Create MySQL DW               | ✅ Done      | Star schema built in MySQL           |
| Tableau Dashboard Integration | ✅ Done      | Connected CSV export to Tableau      |
| Dockerization                 | 🔜 Optional | (Folder structure ready for future)  |
| Orchestration (Airflow)       | 🔜 Optional | (To be added as next phase)          |

---

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Database:** MySQL (via XAMPP)
* **Libraries:** Pandas, SQLAlchemy, mysql-connector-python
* **Dashboard:** Power BI 
* **Tools:** Git, VSCode, Docker (optional)

---

## 🧩 Data Pipeline Overview

### 1. Ingestion (Staging Layer)

Raw `.csv` files from Amazon sales reports are loaded into `stg_amazon_sale_report` and `stg_sale_report` via custom Python scripts.

### 2. Transformation (Dimensional Model)

Scripts transform staging data into the following star schema:

#### Dimension Tables:

* `dim_date`
* `dim_customer`
* `dim_product`
* `dim_platform`
* `dim_order_status`
* `dim_time`

#### Fact Table:

* `fact_sales`

### 3. Data Quality Checks

Custom script `run_data_quality_checks.py` verifies:

* Row count in `fact_sales`
* Missing foreign key violations
* Null currency fields
* Negative quantity/amounts

### 4. Dashboard

Data exported to `.csv` and visualized in Tableau using:

* Sales trends over time
* Top products by revenue
* Orders by platform and location
* KPIs & interactive filters


## 📌 How to Run Locally

1. **Clone the repository**

```bash
git clone https://github.com/your-username/data-engineering-ecommerce-analytics-project.git
```

2. **Install dependencies**

```bash
pip install pandas sqlalchemy mysql-connector-python
```

3. **Start MySQL server via XAMPP**

   * Create a database named `ecommerce_dw`
   * Run `sql/schema.sql` to create tables (optional if automated in scripts)

4. **Run ingestion + transformation**

```bash
python scripts_ingestion/load_staging_data.py
python scripts_transform/transform_dim_date.py
python scripts_transform/transform_fact_sales.py
# (Run all other dimension scripts too)
```

5. **Run Data Quality Check**

```bash
python scripts_transform/run_data_quality_checks.py
```

6. **Export to CSV for Tableau**

```bash
python scripts_transform/export_fact_sales_csv.py
```

---

## 📈 Future Enhancements

* Migrate DW from MySQL to **PostgreSQL**
* Add **Airflow** DAGs for full pipeline orchestration
* Deploy dashboard using **Tableau Public API**
* Containerize with **Docker Compose**
* Integrate with **Cloud (AWS S3 + RDS)**

---

## 📚 Resources

* [Kaggle Amazon Sales Dataset](#) 

---

## 👨‍💻 Author

**Vishal Aryav Bhandary**
