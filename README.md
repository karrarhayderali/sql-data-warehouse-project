
---
## Table of Contents
- [Overview](#overview)
- [Data Warehouse Project](#data-warehouse-project)
- [Data warehouse Architecture](#data-warehouse-architecture)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [How to Run This Project](#how-to-run-this-project)
- [Author & Contact](#author--contact)

---
# OverView
## **Data Warehouse Project**
Developing a **data warehouse** using **SQL Server** to establish a centralized and reliable data repository that supports data analysts in performing advanced analytics, reporting, and data-driven decision-making.


# Data warehouse Architecture

This project focuses on building a **data warehouse** using **SQL Server**, with **CSV files** as the primary data source. The solution follows the **Medallion Architecture** consisting of **Bronze, Silver, and Gold layers**:

* **Bronze Layer:**
  Stores raw data ingested directly from the CSV files without any modifications, preserving the original source data.

* **Silver Layer:**
  Performs all necessary **data transformations, standardization, cleansing, and validation** to produce clean and analytics-ready datasets.

* **Gold Layer:**
  Contains the finalized **business-ready data models**, including **star schema** structures with **fact and dimension tables**, enabling optimized analytics and reporting.

The objective of this project is to establish a scalable and well-organized data pipeline that transforms raw source data into high-quality, analysis-ready datasets for business intelligence use cases.

### Data Loading Strategy

The project uses a Truncate-and-Insert loading method, where all target tables are emptied before loading new data. This ensures:

* A clean refresh of all layers
* Consistency between source and warehouse data
* Simplified data maintenance and validation
---

### Data Flow Diagram
![Data Flow Diagram](images/data%20flow%20diagram.png)


---
### Data Model
![Data Model](images/Data%20Model.png)
--

# Data Sources
The solution integrates data from two source systems, each providing multiple CSV files:

1. CRM Source System

- Contains 3 CSV files:

* crm_sales_details.csv – transactional sales data

* crm_cust_info.csv – customer profile information

* crm_prd_info.csv – product info data

2. ERP Source System

- Contains 3 CSV files:

* erp_cust_az12.csv – ERP customer records

* erp_loc_a101.csv – location of customers (country)

* erp_px_cat_g1v2.csv – product category 
---


# Project Structure
```
Data-Warehouse-Project/
├── datasets/
│   ├── source_crm/
│   │   ├── crm_sales_details.csv
│   │   ├── crm_cust_info.csv
│   │   └── crm_prd_info.csv
│   │
│   ├── source_erp/
│       ├── erp_cust_az12.csv
│       ├── erp_loc_a101.csv
│       └── erp_px_cat_g1v2.csv
│
├── images/
│   ├── Data Flow Diagram.png
│   └── Data Model.png
│
├── scripts/
│   ├── bronze/
│   │   ├── bronze_Insert data BULK_method.sql
│   │   ├── create_bronze_layer_ddl.sql
│   │   └── Stored procedure to load bronze layer.sql
│   │
│   ├── silver/
│   │   ├── create_silver_layer_ddl.sql
│   │   └── Silver_layer_stored_procedure.sql
│   │
│   ├── gold/
│   │   ├── createing_customer_view_from_customers_tables.sql
│   │   ├── createing_product_view_from_all_product_tables.sql
│   │   └── createing_sales_view_from_sales_details.sql
│   │
│   ├── cleansing/
│   │   ├── crm_cust_info_exploring_issues.sql
│   │   ├── crm_prd_info_cleansing.sql
│   │   ├── crm_sales_details_cleansing.sql
│   │   ├── erp_cust_az12_exploring_issues.sql
│   │   ├── erp_loc_a101_exploring_issues.sql
│   │   └── erp_px_cat_g1v2_exploring.sql
│   │
│   └── creating_database.sql
│
│  
│
├── README.md
└── .gitignore
```


# How to Run This Project

1. **Clone the repository**  

   git clone https://github.com/karrarhayderali/sql-data-warehouse-project.git

2. **Create database and schemas **
   SQL `scripts/creating_database.sql`

3. **Creating bronze layer**
   Run `scripts/bronze/create_bronze_layer_ddl.sql` to create bronze layer tables
   Then run `scripts/bronze/bronze_Insert data BULK_method.sql` to insert the data after replace file path with yours, or `scripts/bronze/Stored procedure to load bronze layer.sql` after creating the procedure run it, also need file path change

4.  **Creating silver layer**
   Run `scripts/silver/create_silver_layer_ddl.sql` to create silver layer tables
   Then `scripts/silver/Silver_layer_stored_procedure.sql` after running the code then run the stored procedure to insert data from bronze to silver after cleansing process

5.  **Creating gold layer**
   Run `scripts/gold/createing_customer_view_from_customers_tables.sql`
   Run `scripts/gold/createing_product_view_from_all_product_tables.sql`
   Run `scripts/gold/createing_sales_view_from_sales_details.sql`

## Author & Contact
**Karar Haider – Data Analyst**  
📧 Email: [karrarhayderali@gmail.com](mailto:karrarhayderali@gmail.com)  
🔗 GitHub: [karrarhayderali](https://github.com/karrarhayderali)  
🔗 LinkedIn: [https://www.linkedin.com/in/karrar-hayder-33758a284](https://www.linkedin.com/in/karrar-hayder-33758a284)


