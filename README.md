# Lab_3_Data_preprocessing_Azure

This lab demonstrates a **practical application of the Medallion Architecture**, showing how raw data is transformed through the Bronze, Silver, and Gold layers:

- Bronze Layer: Raw JSON data ingested from public sources and stored in the raw folder.

- Silver Layer: Data Factory converted JSON to Parquet, then Databricks cleaned and validated each dataset.

- Gold Layer: Datasets were joined into the curated_reviews Delta table, refined using Fabrics and Databricks, and saved as the final cleaned table.

### Part 1: Curate the gold table

This part involves curating the Goodreads data using Parquet files from the processed folder, which was created via the Azure Data Factory pipeline from the raw data. The necessary columns were stored in the variable reviews_clean. Columns from the books, authors, and reviews_clean datasets were examined, and a bridge table book_authors was created to link books and authors. The datasets books, authors, book_authors, and reviews_clean were joined into a single curated DataFrame and saved to gold_path. The DataFrame was then converted into a Delta table and stored as a single table named curated_reviews inside the gold folder of the lakehouse, making it available for SQL queries. The table was verified using a simple SQL query. The implementation can be reviewed in the Notebooks folder: Notebooks/01_Curate_Gold_Table.ipynb.

### Part 2 – Fabric Data Cleaning and Aggregation

This part involves four main tables:

1- Source Table (Query): Cleaning steps of the gold table curated_reviews from Part 1.
2- Book Summary Table (agg_book_summary):  Add a new aggregated feature using group by for average book rating and number of reviews per book.
3- Author Summary Table (avg_author_summary): Add a new aggregated feature using group by for avwrage author reviews.
4- Book Review Statistics (book_review_stats): Review word count statistics per book.
5- Final Merged Table (cleaned_dataset): Combines all the above tables using left outer into one cleaned dataset and did final cleaning steps.

**1- Source table steps:**

Key Operations

-Load data from Azure Delta Lake (curated_reviews)
-Parse and convert date_added to ISO format (YYYY-MM-DD)
-Change column types for IDs, ratings, and dates
-Remove empty and null rows (multiple iterations for completeness)
-Add review_length column based on review_text
-Filter reviews with review_length ≥ 10
-Handle errors in date parsing
-Validate date ranges against current date
-Replace nulls and blanks with defaults (n_votes → 0, language_code → "Unknown")
-Trim whitespace from text fields (title, name, review_text)
-Capitalize titles and author names
-Remove temporary/validation columns (e.g., valid_Dates)

Output

-Columns: 13 (review_id, book_id, title, author_id, name, user_id, rating, review_text, language_code, n_votes, date_added, date_added_iso,review_length)
-Quality: Clean, validated, standardized
-Null values: 0%




