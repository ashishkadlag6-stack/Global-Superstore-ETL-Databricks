# 🚀 Global Superstore ETL Pipeline — Databricks

## 📌 Project Overview

An end-to-end **ETL Data Engineering Pipeline** built using **Databricks, PySpark, Delta Lake, and Spark SQL** to process and analyze the Global Superstore dataset.

The project follows the **Medallion Architecture** with **Bronze, Silver, and Gold layers** to ingest raw CSV data, clean and transform it, and create business-ready datasets for analytics and reporting.

The processed data is used to build **Power BI dashboards** for analyzing sales, profit, customers, products, categories, regions, and overall business performance.

---

## 🏗️ Architecture

```text
                    Global Superstore CSV
                            │
                            ▼
                    ┌───────────────┐
                    │   Databricks  │
                    │   PySpark     │
                    └───────┬───────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │    BRONZE LAYER     │
                 │                     │
                 │ Raw CSV Data        │
                 │ Delta Format        │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │    SILVER LAYER     │
                 │                     │
                 │ Data Cleaning       │
                 │ Null Handling       │
                 │ Duplicate Removal   │
                 │ Data Validation     │
                 │ Date Transformation │
                 │ Feature Engineering │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │     GOLD LAYER      │
                 │                     │
                 │ Business KPIs       │
                 │ Sales Analysis      │
                 │ Profit Analysis     │
                 │ Customer Analysis   │
                 │ Product Analysis    │
                 └──────────┬──────────┘
                            │
                    ┌───────┴────────┐
                    ▼                ▼
              Spark SQL          Power BI
              Analytics          Dashboard
```

---

## 🎯 Project Objectives

* Build an end-to-end ETL pipeline using Databricks.
* Process raw Global Superstore CSV data using PySpark.
* Implement Medallion Architecture.
* Store processed data using Delta Lake.
* Perform data cleaning and validation.
* Create business-ready Gold datasets.
* Perform analytical queries using Spark SQL.
* Create Power BI dashboards for business insights.

---

## 🛠️ Technologies Used

| Technology       | Purpose                         |
| ---------------- | ------------------------------- |
| **Databricks**   | Data Engineering & ETL Platform |
| **PySpark**      | Data Processing                 |
| **Apache Spark** | Distributed Data Processing     |
| **Delta Lake**   | Reliable Data Storage           |
| **Spark SQL**    | Data Analytics                  |
| **Python**       | ETL Development                 |
| **CSV**          | Source Data                     |
| **Power BI**     | Data Visualization              |
| **Git & GitHub** | Version Control                 |

---

# 📂 Dataset

The project uses the:

**Global Superstore Uncleaned Dataset**

The dataset contains information about:

* Orders
* Customers
* Products
* Categories
* Sales
* Quantity
* Discount
* Profit
* Countries
* Regions
* Shipping
* Order Dates

### Important Columns

```text
Row_ID
Order_ID
Order_Date
Ship_Date
Ship_Mode
Customer_ID
Customer_Name
Segment
Country
City
State
Postal_Code
Region
Product_ID
Category
Sub_Category
Product_Name
Sales
Quantity
Discount
Profit
```

---

# 🔄 ETL Pipeline

## 1️⃣ Bronze Layer — Raw Data

The raw CSV dataset is ingested into Databricks.

### Operations

* Read CSV using PySpark.
* Infer schema.
* Standardize column names.
* Store raw data in Delta format.

Example:

```python
df = spark.read.csv(
    "/Volumes/bronze/default/bronze1/Global_Superstore_Uncleaned_Dataset.csv",
    header=True,
    inferSchema=True
)
```

Column names are standardized:

```python
for column in df.columns:
    new_column = (
        column.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )

    df = df.withColumnRenamed(column, new_column)
```

The data is then stored as a Delta dataset.

---

# 2️⃣ Silver Layer — Data Cleaning & Transformation

The Silver layer contains cleaned and validated data.

### Data Cleaning Operations

* Remove duplicate records.
* Handle missing values.
* Trim unnecessary spaces.
* Convert data types.
* Convert date columns.
* Standardize categorical values.
* Validate Sales values.
* Validate Quantity.
* Create derived columns.

### Example

```python
silver_df = silver_df.dropDuplicates()
```

Date transformation:

```python
silver_df = silver_df.withColumn(
    "Order_Date",
    to_date(col("Order_Date"), "MM/dd/yyyy")
)
```

Numeric transformation:

```python
silver_df = silver_df.withColumn(
    "Sales",
    col("Sales").cast("double")
)
```

Profit classification:

```python
silver_df = silver_df.withColumn(
    "Profit_Status",
    when(col("Profit") > 0, "Profitable")
    .when(col("Profit") < 0, "Loss")
    .otherwise("Break Even")
)
```

Additional features:

```text
Order_Year
Order_Month
Discount_Amount
Net_Sales
Profit_Status
```

---

# 3️⃣ Gold Layer — Business Analytics

The Gold layer contains business-ready datasets.

### Gold Tables

```text
gold_category
gold_country
gold_monthly
gold_customer
gold_product
```

### Category Performance

```python
gold_category = silver.groupBy(
    "Category"
).agg(
    sum("Sales").alias("Total_Sales"),
    sum("Profit").alias("Total_Profit"),
    sum("Quantity").alias("Total_Quantity"),
    avg("Discount").alias("Average_Discount"),
    countDistinct("Order_ID").alias("Total_Orders")
)
```

---

# 📊 Spark SQL Analytics

The project includes SQL analytics for:

### Sales Analysis

* Total Sales
* Category-wise Sales
* Country-wise Sales
* Monthly Sales
* Regional Sales

### Profit Analysis

* Total Profit
* Category-wise Profit
* Country-wise Profit
* Profit Margin
* Loss-making Orders

### Customer Analysis

* Top 10 Customers
* Customer Revenue
* Customer Orders

### Product Analysis

* Top Products
* Product Sales
* Product Profit

### Example SQL

```sql
SELECT
    Category,
    SUM(Sales) AS Total_Sales,
    SUM(Profit) AS Total_Profit
FROM silver_superstore
GROUP BY Category
ORDER BY Total_Sales DESC;
```

---

# 📈 Power BI Dashboard

The Gold-layer datasets are used for Power BI reporting.

### Dashboard KPIs

```text
┌────────────────┬────────────────┐
│ Total Sales    │ Total Profit   │
├────────────────┼────────────────┤
│ Total Orders   │ Total Quantity │
└────────────────┴────────────────┘
```

### Dashboard Analysis

* Sales Overview
* Profit Overview
* Category Performance
* Regional Performance
* Country Performance
* Monthly Sales Trend
* Top Products
* Top Customers
* Discount Analysis

---

# 📁 Project Structure

```text
Global-Superstore-ETL-Databricks/
│
├── data/
│   └── Global_Superstore_Uncleaned_Dataset.csv
│
├── notebooks/
│   ├── 01_Bronze_Ingestion
│   ├── 02_Silver_Cleaning
│   ├── 03_Gold_Transformation
│   ├── 04_SQL_Analytics
│   └── 05_Data_Quality
│
├── powerbi/
│   └── Global_Superstore_Dashboard.pbix
│
├── screenshots/
│   ├── bronze.png
│   ├── silver.png
│   ├── gold.png
│   └── powerbi_dashboard.png
│
└── README.md
```

---

# 🔍 Data Quality Checks

The pipeline performs data quality validation including:

* Null value detection
* Duplicate detection
* Invalid Sales detection
* Invalid Quantity detection
* Data type validation
* Date validation
* Schema validation

Example:

```python
print("Duplicate Records:",
      silver.count() - silver.dropDuplicates().count())
```

---

# 📊 Business Questions Answered

The project answers questions such as:

1. Which category generates the highest sales?
2. Which category generates the highest profit?
3. Which countries generate the most revenue?
4. Which products have the highest sales?
5. Who are the top customers?
6. What is the monthly sales trend?
7. Which regions generate the highest profit?
8. Which products are generating losses?
9. What is the average order value?
10. Which category has the highest discount?
11. What is the overall profit margin?
12. Which customer segment performs best?

---

# 🚀 Key Features

* End-to-end ETL pipeline
* Medallion Architecture
* PySpark transformations
* Delta Lake storage
* Data quality validation
* Spark SQL analytics
* Business KPI generation
* Power BI reporting
* GitHub-ready project structure

---

# 💼 Resume Description

**Global Superstore ETL Pipeline | Databricks, PySpark, Delta Lake, Spark SQL, Power BI**

> Developed an end-to-end ETL pipeline in Databricks using PySpark and Delta Lake to process Global Superstore data. Implemented Medallion Architecture with Bronze, Silver, and Gold layers for data ingestion, cleansing, transformation, validation, and business aggregation. Performed analytical queries using Spark SQL and developed Power BI dashboards to visualize sales, profit, customer, product, and regional performance.

---

# 📚 Skills Demonstrated

```text
Databricks
PySpark
Apache Spark
Spark SQL
Delta Lake
ETL
Data Engineering
Medallion Architecture
Data Cleaning
Data Transformation
Data Quality
SQL Analytics
Power BI
Python
Git
GitHub
```

---

# 👨‍💻 Author

**Ashish Kadlag**

Data Science | Data Engineering | Machine Learning

---

⭐ If you found this project useful, consider giving the repository a star!
