# Databricks Lakehouse Project 2026

Welcome to the **Databricks Lakehouse Project 2026** repository! 🚀
This project demonstrates a comprehensive datalakehouse solution using the cloud-based data intelligence platform, Databricks. It is designed as a portfolio project, and highlights industry best practices in data engineering.

---

## 🚀 Project Requirements

### Building the Data Lakehouse

#### Objective
Develop a data lakehouse using Databricks to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

## BI: Analytics & Reporting

#### Objective
Develop analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling key decision-making.

---

# Data Architecture 🏗️
<img width="994" height="1002" alt="data_lakehouse drawio" src="https://github.com/user-attachments/assets/655127e7-9df7-4c66-b00d-f1f58ba11184" />

1. **Bronze Layer:** Stores raw data as-is from the source systems. Data is ingested from CSV files into Databricks database.
2. **Silver Layer:** This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3.  **Gold Layer:** Houeses business-ready data modeled into a star schema required for reporting and analytics.

---

## 🛡️ License

This project is licensed under the MIT License do feel free to use, modify, and share this project with proper attribution.

## 🧑‍🦰 About Me

Hi! I'm Kevin Botelho, an aspiring data engineer who enjoys coding!
