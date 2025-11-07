# Lab_3_Data_preprocessing_Azure

This lab demonstrates a **practical application of the Medallion Architecture**, showing how raw data is transformed through the Bronze, Silver, and Gold layers:

- Bronze Layer: Raw JSON data ingested from public sources and stored in the raw folder.

- Silver Layer: Data Factory converted JSON to Parquet, then Databricks cleaned and validated each dataset.

- Gold Layer: Datasets were joined into the curated_reviews Delta table, refined using Fabrics and Databricks, and saved as the final cleaned table.

### Part 1: Curate the gold table

This part involves curating the Goodreads data using Parquet files from the processed folder, which was created via the Azure Data Factory pipeline from the raw data. The necessary columns were stored in the variable reviews_clean. Columns from the books, authors, and reviews_clean datasets were examined, and a bridge table book_authors was created to link books and authors. The datasets books, authors, book_authors, and reviews_clean were joined into a single curated DataFrame and saved to gold_path. The DataFrame was then converted into a Delta table and stored as a single table named curated_reviews inside the gold folder of the lakehouse, making it available for SQL queries. The table was verified using a simple SQL query. The implementation can be reviewed in the Notebooks folder: Notebooks/01_Curate_Gold_Table.ipynb.



