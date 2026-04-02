# 🚀 Customer Data Platform

An end-to-end data engineering project implementing the **Medallion Architecture (Bronze → Silver → Gold)** using **PySpark**.

This platform ingests and integrates data from multiple sources (**CRM & ERP systems**) and transforms it into analytics-ready datasets.

---

## 📌 Overview

The goal of this project is to design a scalable data platform that:

* Ingests raw data from different systems
* Cleans and standardizes inconsistent formats
* Integrates datasets into a unified model
* Prepares data for analytics and reporting

---

## 🏗️ Architecture

```
        ┌────────────┐
        │   Raw CSV      │
        └─────┬──────┘
              ↓
        🥉 Bronze Layer
        (Raw Ingestion)
              ↓
        🥈 Silver Layer
   (Clean + Transform + Join)
              ↓
        🥇 Gold Layer
     (Business Aggregates)
```

---

## 🧰 Tech Stack

* **PySpark** (data processing)
* **Python**
* **Hadoop (Windows setup with winutils)**
* **Parquet** (storage format)

### 🔜 Planned

* dbt (data modeling)
* Airbyte (data ingestion)
* Databricks (cloud platform)
* Power BI (dashboarding)

---

## 📂 Project Structure

```
customer-data-platform/
│
├── data/
│   ├── crm/              # Raw CRM CSV files
│   ├── erp/              # Raw ERP CSV files
│   ├── bronze/           # Raw ingested parquet
│   ├── silver/           # Cleaned & transformed data
│   └── gold/             # Aggregated business data
│
├── notebooks/
│   ├── bronze/
│   │   ├── bronze_erp_loc_a101.py
│   │   ├── bronze_erp_cust_az12.py
│   │   ├── bronze_crm_cust_info.py
│   │   ├── bronze_crm_prd_info.py
│   │   └── bronze_crm_sales_details.py
│   │
│   ├── silver/
│   │   ├── silver_erp_loc_a101.py
│   │   ├── silver_erp_cust_az12.py
│   │   ├── silver_crm_cust_info.py
│   │   ├── silver_crm_prd_info.py
│   │   ├── silver_crm_sales_details.py
│   │   └── silver_customer.py   # 🔥 unified customer
│   │
│   └── gold/
│       └── (coming soon)
│
└── README.md
```

---

## 🥉 Bronze Layer (Raw Ingestion)

* Loads raw CSV files **as-is**
* No transformations applied
* Stores data in **Parquet format**

### Data Sources

**ERP**

* `CUST_AZ12.csv` → customer demographics
* `LOC_A101.csv` → customer location
* `PX_CAT_G1V2.csv` → product categories

**CRM**

* `cust_info.csv` → customer profile
* `prd_info.csv` → product information
* `sales_details.csv` → sales transactions

---

## 🥈 Silver Layer (Transformation & Integration)

This is where the real engineering happens 💪

### Key transformations:

* Column standardization (snake_case)
* Trimming & cleaning dirty strings
* Date formatting
* Deduplication
* Business logic mapping (e.g., marital status)
* Data joins across systems

---

### 🔥 Unified Customer Table

The highlight of the project:

`silver_customer.py`

Combines:

* ERP customer data (birthdate, gender)
* CRM customer data (name, marital status)

### Result:

A **single customer view**:

```
customer_id
first_name
last_name
gender
birth_date
marital_status
```

---

## 🥇 Gold Layer (Coming Next)

This layer will provide business-level insights such as:

* Customer segmentation
* Sales performance
* Revenue by product/category
* Customer distribution by country

---

## ✅ Current Progress

* [x] Project setup
* [x] Bronze layer (ERP + CRM ingestion)
* [x] Silver layer (cleaning & transformation)
* [x] Unified customer table (cross-source join)
* [ ] Gold layer (aggregations)
* [ ] dbt integration
* [ ] Airbyte ingestion
* [ ] Dashboard / visualization

---

## ▶️ How to Run

### 1️⃣ Bronze Layer

Run ingestion scripts:

```
python notebooks/bronze/bronze_erp_loc_a101.py
python notebooks/bronze/bronze_erp_cust_az12.py
python notebooks/bronze/bronze_crm_cust_info.py
```

---

### 2️⃣ Silver Layer

Run transformation scripts:

```
python notebooks/silver/silver_erp_loc_a101.py
python notebooks/silver/silver_erp_cust_az12.py
python notebooks/silver/silver_crm_cust_info.py
python notebooks/silver/silver_customer.py
```

---

### 3️⃣ Output

Data will be written to:

```
data/silver/
data/gold/
```

---

## 📌 Notes

* Data files are excluded via `.gitignore`
* Spark outputs multiple `part-*` files + `_SUCCESS` (expected behavior)
* Joins require careful column disambiguation (e.g., duplicate `gender` columns)

---

## 🎯 Future Improvements

* Add **Gold layer aggregations**
* Introduce **dbt models**
* Automate ingestion using **Airbyte**
* Deploy on **Databricks**
* Build **Power BI dashboard**

---

## 👤 Author

**Abel Solomon**

Data Engineer | PySpark | Building scalable data platforms 🚀
