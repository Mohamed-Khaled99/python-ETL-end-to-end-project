# Python End-to-End ETL Pipeline Project

## 📌 Project Overview
This project implements a complete **End-to-End ETL (Extract, Transform, Load) Pipeline** using **Python**, simulating a real-world **Data Engineering** scenario.

The pipeline extracts data from multiple heterogeneous sources (API, MySQL Database, and Local Data Lake), applies data quality checks and business transformations, builds a **dimensional data model (Star Schema)**, and generates **business insights** through data visualization.

The project follows best practices in data engineering such as modular design, staging layers, metadata enrichment, and clear separation of responsibilities between pipeline phases.

<img width="1475" height="975" alt="PyProject_Architicture" src="https://github.com/user-attachments/assets/ada0283d-fa69-48b6-8be3-232a809ed6fc" />


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

  <img width="1102" height="922" alt="Totalsales per month" src="https://github.com/user-attachments/assets/3f200c0d-1fad-496c-a979-2fa388d6b884" />
<img width="1131" height="642" alt="total sales per year" src="https://github.com/user-attachments/assets/b23d1503-ba3b-4a53-a6e4-23c0898ceb40" />
<img width="1291" height="961" alt="Screenshot 2026-01-05 202416" src="https://github.com/user-attachments/assets/20e68c05-1520-4722-9583-102277c55b38" />
<img width="900" height="766" alt="sales per staff" src="https://github.com/user-attachments/assets/d1677985-ceae-4ad5-962f-5f40f875288b" />
<img width="1128" height="870" alt="Top 10 Products" src="https://github.com/user-attachments/assets/b429f2fc-ffaa-4bcb-bff3-54944fa8c9a1" />
<img width="907" height="645" alt="sales per store region" src="https://github.com/user-attachments/assets/05c5ec82-b8f6-4d09-8593-1c5d83a149ad" />
---
 
- Generated business insights using:
  - Time-series analysis (Sales Over Time)
  - Top-N analysis (Best Selling Products)
  - Distribution analysis (Customer Segmentation)
- Visualizations created using:
  - `matplotlib`
 <img width="640" height="480" alt="Sales over month" src="https://github.com/user-attachments/assets/632c8137-105c-4eb2-9b47-dc2b21632ee5" />
<img width="640" height="480" alt="Sales over Days" src="https://github.com/user-attachments/assets/c40f56f9-0c23-4f36-ad73-9220e67350d3" />
<img width="640" height="480" alt="Sales over year" src="https://github.com/user-attachments/assets/73a0cdf7-ca8d-488b-b9e3-fe3ea2217100" />
<img width="640" height="480" alt="sales per store state" src="https://github.com/user-attachments/assets/f40287be-9d59-43bb-b58a-33cc0f2af0a9" />
<img width="640" height="480" alt="Top 10 Products chart" src="https://github.com/user-attachments/assets/2eecfb1c-8230-4380-b6da-33dcfdec91e1" />

## 🗂️ Project Structure

