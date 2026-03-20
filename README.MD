\# Customer Data Platform



🚧 Work in progress



This project aims to build an end-to-end data pipeline using:



\* CRM and ERP data sources

\* Medallion architecture (Bronze, Silver, Gold)

\* Tools: Databricks, Spark, dbt, Airbyte



\## Current Progress



\* \[x] Project structure created

\* \[x] Raw data collected

\* \[ ] Bronze layer

\* \[ ] Silver layer

\* \[ ] Gold layer



\---



\## 🥉 Bronze Layer Design



The Bronze layer stores raw data ingested from source systems without transformation.



\### Sources:



\* CRM system

\* ERP system



\### Tables:



\*\*CRM\*\*



\* bronze\_crm\_cust\_info

\* bronze\_crm\_prd\_info

\* bronze\_crm\_sales\_details



\*\*ERP\*\*



\* bronze\_erp\_cust\_az12

\* bronze\_erp\_loc\_a101

\* bronze\_erp\_px\_cat\_g1v2



This design ensures traceability, scalability, and separation of source systems.



