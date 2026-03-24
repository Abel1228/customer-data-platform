\# Customer Data Platform



This project implements an end-to-end data pipeline using the Medallion Architecture (Bronze, Silver, Gold) with PySpark.



\---



\## 🚀 Overview



The goal of this project is to design and build a scalable data platform that ingests, transforms, and aggregates data from multiple sources (CRM \& ERP) into business-ready insights.



\---



\## 🏗️ Architecture



```

Raw Data → Bronze → Silver → Gold

&#x20;        (CSV)   (Parquet)  (Aggregated Insights)

```



\* \*\*Bronze Layer\*\* → Raw ingestion (no transformation)

\* \*\*Silver Layer\*\* → Cleaned and transformed data

\* \*\*Gold Layer\*\* → Business-level aggregations



\---



\## 🧰 Tech Stack



\* PySpark

\* Hadoop (Windows setup with winutils)

\* Python

\* (Planned) dbt, Airbyte, Databricks



\---



\## 📂 Project Structure



```

customer-data-platform/

│

├── data/

│   ├── crm/

│   ├── bronze/

│   ├── silver/

│   └── gold/

│

├── notebooks/

│   ├── bronze/

│   │   └── bronze\_ingestion.py

│   ├── silver/

│   │   └── silver\_transform.py

│   └── gold/

│       └── gold\_aggregation.py

│

└── README.md

```



\---



\## 🥉 Bronze Layer



\* Ingests raw CRM data from CSV

\* Stores data as-is in partitioned format



Example:



\* `bronze\_crm\_cust\_info`



\---



\## 🥈 Silver Layer



\* Cleans and transforms raw data

\* Standardizes column names

\* Derives new fields



Example transformation:



\* `cst\_marital\_status → marital\_status (Married / Single)`



\---



\## 🥇 Gold Layer



\* Aggregates data for business use



Example:



\* Customer count by marital status



\---



\## ✅ Current Progress



\* \[x] Project setup

\* \[x] Bronze layer (ingestion)

\* \[x] Silver layer (transformation)

\* \[x] Gold layer (aggregation)

\* \[ ] ERP data integration

\* \[ ] dbt modeling

\* \[ ] Airbyte ingestion

\* \[ ] Dashboard / visualization



\---



\## ▶️ How to Run



\### 1. Bronze Layer



```

python notebooks/bronze/bronze\_ingestion.py

```



\### 2. Silver Layer



```

python notebooks/silver/silver\_transform.py

```



\### 3. Gold Layer



```

python notebooks/gold/gold\_aggregation.py

```



\---



\## 📌 Notes



\* Data files are excluded using `.gitignore`

\* Spark generates multiple output files (part files + \_SUCCESS), which is expected behavior



\---



\## 🎯 Future Improvements



\* Integrate Airbyte for ingestion

\* Use dbt for transformations

\* Load data into SQL Server

\* Build Power BI dashboard



\---



\## 💡 Author



Abel Solomon



