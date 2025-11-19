# Lab 2 – Climate Data ETL and Analysis

**Lab Overview**
This lab is divided into two parts:
- SQL Exploration: Querying and exploring the dataset using SQL to understand the data structure and basic statistics.
- Python Analysis: Analyzing temperature and precipitation trends, including year-to-year comparisons and similar analysis for another town, to observe changes in weather patterns over time.

The lab also covers a simple ETL process to prepare the climate data for analysis.

**ETL Process**
- Extract:
  - Downloaded three CSV files (one per year) and stored them in Azure Blob Storage.
  - Created two datasets: ghcn_raw (for running SQL queries) and ghcn_processed (for cleaned and processed data).
  - Set up a connection to Azure Blob Storage using a SAS token to safely read and write files.

- Transform:
  - Converted raw CSV data in ghcn_processed into Parquet format, which is smaller, faster, and optimized for analysis in Python notebooks.
  - The resulting table was named climate_parquet.

- Load
  - Saved the climate_parquet table in the cloud so it could be accessed in Python notebooks for analysis.
  - The ghcn_raw dataset was retained for SQL-based exploration of the original data.

**Analysis Steps**

- Used ghcn_raw to run SQL queries and explore the dataset, e.g., counting rows, computing average temperature and precipitation at a station.
- Used ghcn_processed in Python to study yearly trends in temperature and precipitation, calculating year-to-year differences.
- Checked the first few temperature and precipitation records to understand the dataset better.
- Focused on a specific town to analyze its temperature and precipitation data for deeper insights.

**Notebook**
- The full implementation is available in this branch: 01_lab2.ipynb
