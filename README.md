# ETL Workshop and Hiring Analysis

## Introduction
This project is part of a workshop that demonstrates a complete data workflow, starting with an exploratory data analysis (EDA), building a dimensional model, and implementing a simple ETL pipeline to load data into a MySQL Data Warehouse. Finally, the processed data is used to analyze key hiring KPIs.

## Workflow
1. **Data Source**  
   The process begins with a CSV file that was explored with an EDA to understand its structure and contents.  

2. **Dimensional Model & Database**  
   Based on the EDA results, a dimensional model was built with one fact table and five dimension tables. The schema was implemented in MySQL.  

3. **ETL Process**  
   - **Extraction:** Data is taken from the initial CSV file.  
   - **Transformation:** Data cleaning and reshaping into dimension-specific DataFrames.  
   - **Loading:** Transformed data is inserted into the Data Warehouse for querying and analysis.  

## KPI Analysis
- **Hires by technology:** Highlights the most in-demand technologies and professions.  
- **Hires by year:** The year 2022 shows fewer records, affecting overall trends.  
- **Hires by seniority:** Slight preference for lower-salary profiles, though hiring is generally balanced across levels.  
- **Hires by country:** Four countries stand out with the highest number of hires, especially in 2019 and 2021.  
- **Conversion rate (applications → hires):** The 2022 conversion rate is unusually high due to limited data.  
- **Average test scores by year:** 2021 had the highest average scores, while 2022 again shows anomalies due to fewer records.  

## Project Components
- **`datawarehouse.py`** → Helper functions (DB connection, data loading utilities).  
- **`transform.py`** → Transformation logic to create dimension DataFrames.  
- **`load.py`** → Responsible for loading cleaned/transformed data into the Data Warehouse.  
- **`main.py`** → Orchestrates the pipeline, running extraction → transformation → loading.  
- **`KPIs.ipynb`** → Notebook with exploratory analysis and KPI visualizations.  

