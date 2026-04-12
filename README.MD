# 🚀 Customer Data Platform

An end-to-end Data Engineering project implementing the Medallion Architecture (Bronze → Silver → Gold) using PySpark.

This platform ingests, transforms, and integrates data from multiple systems (CRM & ERP) into analytics-ready datasets and business insights. It demonstrates how raw data can be converted into meaningful information through structured data engineering processes.

The pipeline is designed to simulate a real-world data platform where data flows through three layers. The Bronze layer handles raw ingestion, the Silver layer performs cleaning and transformation, and the Gold layer produces aggregated insights for business use.

Architecture flow:

Raw CSV → Bronze → Silver → Gold
(CSV) → (Parquet) → (Cleaned Data) → (Aggregated Insights)

The Bronze layer loads raw CSV data from CRM and ERP systems and stores it as-is in Parquet format without applying transformations. This ensures that the original data is always preserved.

The Silver layer is where the core data engineering work happens. Data is cleaned, standardized, and transformed. This includes fixing inconsistent formats, renaming columns to snake_case, handling null values, formatting dates, and applying business logic. Data from different systems is also joined together to create unified datasets.

One of the key outputs of the Silver layer is the unified customer table. This combines ERP customer data (such as birth date and gender) with CRM customer data (such as name and marital status) into a single, clean customer view.

The Gold layer produces business-level insights by aggregating the cleaned data. In this project, a customer sales summary is created by joining customer data with sales data and calculating key metrics.

The Gold layer includes:

* Total spending per customer
* Total number of orders
* Average order value
* Customer segmentation (VIP, Regular, Low Value)

Customer segmentation is based on spending behavior and helps identify high-value customers.

The project uses the following technologies:

* PySpark for distributed data processing
* Python
* Hadoop (Windows setup with winutils)
* Parquet as the storage format

Planned improvements include integrating dbt for data modeling, Airbyte for ingestion, Databricks for cloud execution, and Power BI for dashboards.

Project structure:

customer-data-platform/

data/
├── crm/ (raw CRM CSV files)
├── erp/ (raw ERP CSV files)
├── bronze/ (raw ingested parquet data)
├── silver/ (cleaned and transformed data)
└── gold/ (aggregated business data)

notebooks/
├── bronze/ (ingestion scripts)
├── silver/ (transformation scripts)
└── gold/ (aggregation scripts)

data/filechecker.py (data validation script)
README.md

The pipeline is executed in stages:

Bronze layer ingestion scripts load raw data into parquet format.

Silver layer scripts clean and transform the data, and create the unified customer dataset.

Gold layer scripts generate aggregated business metrics such as total spending and customer segmentation.

To run the project:

python notebooks/bronze/bronze_erp_loc_a101.py
python notebooks/bronze/bronze_erp_cust_az12.py
python notebooks/bronze/bronze_crm_cust_info.py

python notebooks/silver/silver_erp_loc_a101.py
python notebooks/silver/silver_erp_cust_az12.py
python notebooks/silver/silver_crm_cust_info.py
python notebooks/silver/silver_crm_sales_details.py
python notebooks/silver/silver_customer.py

python notebooks/gold/gold_customer_sales_summary.py

To validate results:

python data/filechecker.py

During development, several real-world data engineering challenges were encountered and solved. These include mismatched data types between systems (string vs integer IDs), ambiguous column names after joins, and empty outputs caused by incorrect join conditions. These issues were resolved through proper casting, column selection, and debugging using schema inspection.

Important notes:

* Data files are excluded using .gitignore
* Spark outputs multiple part files and a _SUCCESS file (this is expected)
* Joins require careful handling of data types and column names

Future improvements include adding more Gold-level aggregations, integrating dbt, automating ingestion with Airbyte, deploying on Databricks, and building dashboards in Power BI.

Author:

Abel Solomon
Data Engineer | PySpark | Building scalable data platforms 🚀
