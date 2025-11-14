# Ecommerce Dataset using AWS S3, Snowflake, dbt, and Power BI

[![AWS](https://img.shields.io/badge/AWS-S3-orange)](https://aws.amazon.com/s3/) 
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-blue)](https://www.snowflake.com/) 
[![dbt](https://img.shields.io/badge/dbt-Transformations-red)](https://www.getdbt.com/) 
[![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-yellow)](https://powerbi.microsoft.com/)

This project uses the **Ecommerce Behavior Data from a Multi-Category Store** dataset from Kaggle.

This is **Part 2** of my end-to-end data engineering and analytics pipeline, transitioning from an on-premises workflow to a fully cloud-based architecture.

---

## 🏗️ Cloud Architecture Overview

- **AWS S3** is used as the raw data landing zone where monthly ecommerce data files are uploaded.
- Data is then loaded into **Snowflake**, which acts as the cloud data warehouse and single source of truth.
- **dbt (Data Build Tool)** sits on top of Snowflake and handles SQL-based transformations, documentation, data testing, and model orchestration.
- Final curated tables are connected to **Power BI**, where dashboards were built for analysis, segmentation, and trend insights.

---

## 🔧 What dbt Does in This Project

dbt is the core transformation layer of the pipeline. I used dbt to:

- Transform raw **S3 → Snowflake** data into clean, analytics-ready models across **raw**, **dev**, and **analytics** layers.
- Create modular SQL models that are version-controlled and easy to maintain.
- Automate dependencies using **DAGs**, so transformations run in the correct order.
- Run data quality tests including:
  - `not_null`
  - `unique`
  - `relationships` (referential integrity between fact and dimension tables)
- Build documentation with auto-generated **lineage graphs**.
- Enable **incremental pipelines**, so only new monthly data gets processed instead of reloading everything.
- Promote consistency by using **Jinja + macros** to manage repeated logic.

---

## 📊 dbt Model Breakdown

### 1. Raw Models (Raw Layer)
- Load raw CSV data from Snowflake stage
- Basic cleaning (types, trimming, null handling)
- Preserve grain of raw transactional events

### 2. Dev Models (Cleansed Layer)
- Normalize event names and categories
- Extract `event_date`, `event_time_only`, `day_of_week`, etc.
- Handle duplicates and filter out invalid records

### 3. Analytics Models (Fact & Dimension Layer)
- **Fact Ecommerce Events:** Consolidated table for product views, carts, transactions
- **Dimensions:**
  - `dim_product`
  - `dim_date`

### 4. Tests
- `unique` + `not_null` on primary keys
- `relationships` to ensure referential integrity
- Custom tests:
  - Valid product category list
  - No negative quantity or price
  - Event types must match known values

---

## 🔄 End-to-End Flow

1. Upload monthly ecommerce files → **S3 Bucket**
2. Load raw files → **Snowflake Stage → Raw Schema**
3. Perform cleaning, validation, and modeling → **dbt**
4. Publish analytics tables (facts & dims) → **Snowflake**
5. Connect Snowflake to **Power BI** and build dashboards for sales, product performance, and user behavior funnel analysis

---

## 📈 Power BI Dashboard Highlights

- Funnel metrics: product views → add to cart → purchase
- Conversion rates by day-of-week and time-of-day
- Top-performing products and categories
- Monthly user session trends
- Event type distribution
- Segmentation by user behavior

---

## 🧪 Technologies & Skills Showcased

- **Cloud & Data Engineering:** AWS S3, Snowflake, dbt Core, incremental pipelines
- **Data Modeling:** Raw → Dev → Analytics layers, Fact & Dimension tables, Star schema
- **Analytics & Visualization:** Power BI, DAX, dashboard development
- **Software Engineering:** GitHub version control, modular SQL, documentation, testing

---

## 📁 Project Repository

🔗 **GitHub:** [https://github.com/Maryanne-Cher/ecommerce_dataset](https://github.com/Maryanne-Cher/ecommerce_dataset)






