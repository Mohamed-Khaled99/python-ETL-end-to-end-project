# Python End-to-End ETL Pipeline Project

## 📌 Project Overview
This project implements a complete **End-to-End ETL (Extract, Transform, Load) Pipeline** using **Python**, simulating a real-world **Data Engineering** scenario.

The pipeline extracts data from multiple heterogeneous sources (API, MySQL Database, and Local Data Lake), applies data quality checks and business transformations, builds a **dimensional data model (Star Schema)**, and generates **business insights** through data visualization.

The project follows best practices in data engineering such as modular design, staging layers, metadata enrichment, and clear separation of responsibilities between pipeline phases.

---
## 🎯 Project Objectives
- Extract data from different sources using Python
- Perform data quality checks and cleansing
- Apply business transformations and enrichment
- Design and build a dimensional data model for analytics
- Generate BI insights using charts and visualizations

---

## 📥 Data Sources

### 1️⃣ API
- **OpenExchangeRates API**
- Used to retrieve currency exchange rates for price conversion

### 2️⃣ Relational Database (MySQL)
- Extracted using **mysql.connector**
- Tables:
  - `orders`
  - `order_items`

### 3️⃣ Data Lake (Local File System)
CSV datasets simulating a data lake environment:
- `customers.csv`
- `products.csv`
- `brands.csv`
- `categories.csv`
- `stores.csv`
- `staffs.csv`
- `stocks.csv`

---

## 🔄 ETL Pipeline Phases

### 1️⃣ Extraction
- Data extracted from:
  - REST API
  - MySQL database using explicit SQL queries
  - Local CSV files
- Metadata added:
  - `extraction_timestamp`
  - `data_source`
- Raw data stored in structured folders

---

### 2️⃣ Data Quality Checks
- Null value validation
- Duplicate detection and removal
- Data type and value range validation
- Cleaned data stored in **staging_1**

---

### 3️⃣ Transformation
Key transformations include:
- Currency conversion using latest exchange rates
- Delivery performance metrics:
  - Late delivery flag
  - Delivery latency (days)
- Customer locality classification
- Lookup table resolution for order statuses

Transformed data stored in **staging_2**

---

### 4️⃣ Data Modeling
  - Designed a **Star Schema** for analytics
  - Built:
  - Fact tables for transactional data
  - Dimension tables for descriptive attributes
  - Integrated orders, order items, and product data
  - Final analytical datasets stored in **Information Mart**

    <img width="1111" height="1044" alt="Pyproject_Schema drawio" src="https://github.com/user-attachments/assets/64da99db-00ab-42b3-b8b9-ad9f4e5ae741" />


---

### 5️⃣ Reporting & Visualization
- Generated business insights using:
  - Time-series analysis (Sales Over Time)
  - Top-N analysis (Best Selling Products)
  - Distribution analysis (Customer Segmentation)
- Visualizations created using:
  - `matplotlib`
  - `seaborn`

---

## 🗂️ Project Structure

