# Automated-retail-analytics-etl

This project implements an end-to-end automated ETL (Extract, Transform, Load) pipeline for BusinessRetailDB, designed to streamline the ingestion, cleaning, and storage of retail sales data from multiple sources into a SQL Server database.

Once data is loaded, Power BI dashboards are automatically refreshed, providing up-to-date analytics without any manual intervention.
---
## Table of Contents
- [Project Overview](#project-overview)
- [Data Sources](#data-sources)
- [Tools](#tools)
- [Key Features](#key-features)
- [ETL Workflow](#etl-workflow)
- [Logging and Monitoring](#logging-and-monitoring)
- [Recommendations](#recommendations)


### project overview
---
This project automates the process of loading product-level and summary level sales data into a SQL Server database for retail analytics.

It provides:

Automated Data Cleaning and Validation: Ensures data consistency and quality without manual intervention.

Duplicate Prevention: Uses file hashing to detect and skip previously loaded files.

Seamless Database Integration: Loads clean data directly into SQL Server for downstream analytics.

Scheduled Automation: Refreshes data automatically every 2 hours without any manual effort.

Real-Time Reporting: Power BI dashboards are connected directly to the SQL Server database, updating automatically as new data arrives, so users always have access to the latest insights.

The system is built for Business Intelligence (BI) and data warehousing environments, enabling reliable dashboards, performance tracking, and trend analysis with minimal manual maintenance.

### Data sources 
---
The pipeline ingests CSV files from two main folders:
Product Sales Data (product_sales folder)
Contains transactional-level details for each product sale (e.g., product ID, quantity,discount, gross sales, net sales etc.).
Summary Sales Data (summary_sales folder)
Aggregated data across  product categories, or time period.
Each file dropped into these folders is automatically detected, validated, and loaded into the database.

### Tools
---
- Python Orchestration, automation, and ETL logic

- Pandas  Data wrangling, cleaning, and transformation

- SQLAlchemy  Database connectivity and SQL operations

- ODBC Driver 17 for SQL Server Database driver

- Windows Task Scheduler  Task automation (every 2 hours)

- Hashlib  File fingerprinting to avoid duplicate loads

- Subprocess  System  level automation for scheduling
- Power BI  Live dashboards connected to the database for real-time reporting
 
### key features
---
- Automated File Loading
Scans designated folders for new CSV files and loads them into SQL Server.

- Data Cleaning & Validation
Standardizes column names, fills missing numeric values, and ensures data quality.

- Duplicate Detection via Hashing
MD5 hashing prevents the reloading of already processed files.

- Scheduled Automation
Runs every 2 hours automatically without manual intervention.

- Real-Time Reporting
Power BI dashboards refresh automatically as new data is loaded, providing live insights.

- Comprehensive Logging
Maintains logs of all operations, including loaded file names, row counts, and timestamps.

### ETL Workflow

1. Extraction:

Scans product_sales and summary_sales folders for new CSV files.

2. Transformation:

Cleans column names, fills missing values, and adds metadata (Source_File, Load_Timestamp, File_Drop_Time).

3. Loading:

Loads processed data into SQL Server tables: ProductSales and SummarySales.

Updates loaded_files_hash.csv to track processed files.

4. Automation:

Registers a Windows Task Scheduler job to run the pipeline every 2 hours.

5. Reporting:

Power BI dashboards refresh automatically from the SQL Server tables, showing the latest sales insights without additional manual work.

### Logging and Monitoring

- Log File: pipeline_log.txt  Records timestamps, filenames, and row counts for all loads.

- Hash Log: loaded_files_hash.csv  Tracks file hashes and prevents duplicate ingestion.

- Console Output: Provides real time feedback during ETL execution, including data previews and error handling.
- Integrate additional Power BI reports to visualize trends, product performance, and regional sales metrics.Power BI, Tableau) for real-time analytics.
### Recommendations

- Maintain consistent file naming conventions and schema structure.

- Periodically review pipeline logs for error tracking or performance insights.

- Consider adding data validation rules for business logic checks (e.g., sales thresholds).

- Expand to include email or Slack notifications for pipeline success/failure alerts.

