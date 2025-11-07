# Lab_3_Data_preprocessing_Azure

This lab demonstrates a **practical application of the Medallion Architecture**, showing how raw data is transformed through the Bronze, Silver, and Gold layers:
- Bronze Layer: Raw JSON data ingested from public sources and stored in the raw folder.
- Silver Layer: Data Factory converted JSON to Parquet, then Databricks cleaned and validated each dataset.
- Gold Layer: Datasets were joined into the curated_reviews Delta table, refined using Fabrics and Databricks, and saved as the final cleaned table.

### Part 1: Curate the gold table

This part involves curating the Goodreads data using Parquet files from the processed folder, which was created via the Azure Data Factory pipeline from the raw data. The necessary columns were stored in the variable reviews_clean. Columns from the books, authors, and reviews_clean datasets were examined, and a bridge table book_authors was created to link books and authors. The datasets books, authors, book_authors, and reviews_clean were joined into a single curated DataFrame and saved to gold_path. The DataFrame was then converted into a Delta table and stored as a single table named curated_reviews inside the gold folder of the lakehouse, making it available for SQL queries. The table was verified using a simple SQL query. The implementation can be reviewed in the Notebooks folder: **`Notebooks/01_Curate_Gold_Table.ipynb`**.

### Part 2 – Data Cleaning and Aggregation: Using Microsoft Fabrics

This part involves four main tables:
1. Source Table (Query): Cleaning steps of the gold table curated_reviews from Part 1.
2. Book Summary Table (agg_book_summary):  Add a new aggregated feature using group by for average book rating and number of reviews per book.
3. Author Summary Table (avg_author_summary): Add a new aggregated feature using group by for avwrage author reviews.
4. Book Review Statistics (book_review_stats): Review word count statistics per book.
5. Final Merged Table (cleaned_dataset): Combines all the above tables using left outer into one cleaned dataset and did final cleaning steps.

**1- Source table steps (Query)**
Key Operations
- Load data from Azure Delta Lake (curated_reviews)
- Parse and convert date_added to ISO format (YYYY-MM-DD)
- Change column types for IDs, ratings, and dates
- Remove empty and null rows (multiple iterations for completeness)
- Add review_length column based on review_text
- Filter reviews with review_length ≥ 10
- Handle errors in date parsing
- Validate date ranges against current date
- Replace nulls and blanks with defaults (n_votes → 0, language_code → "Unknown")
- Trim whitespace from text fields (title, name, review_text)
- Capitalize titles and author names
- Remove temporary/validation columns (e.g., valid_Dates)

Output
- Columns: 13 (review_id, book_id, title, author_id, name, user_id, rating, review_text, language_code, n_votes, date_added, date_added_iso,review_length)

**2- Groupby table: Book Summary Table (agg_book_summary)**
Key Operations
- Group reviews by book_id
- Calculate average rating per book from all user ratings
- Count total number of reviews per book
- Convert book_id to text type for consistency
- Remove rows with errors (invalid book_id references)
- Remove empty/null rows for data quality

Output
- Columns: 3 (book_id, avg_rating, num_reviews)

**3 - Groupby Table: Author summary Table**
Key Operations
- Group reviews by author name
- Calculate average rating received by author across all their books

Output
- Columns: 2 (name, avg_author_rating)

**4 - Groupby Table: Count Book review statistics (book_review_stats)**
Key Operations
- Add new column: review_word_count (To count the number of review words)
- Group statistics by book_id
- Calculate average words per review
- Find minimum word count (shortest review for each book)
- Find maximum word count (longest review for each book)

Output
- Columns: 4 (book_id, avg_review_words, min_review_words, max_review_words)

**5 - Final Merged and cleaned Table**
Key Operations
- Joined the main Query with Book Summary (agg_book_summary) using book_id as the key
- Expanded aggregated columns: avg_rating, num_reviews
- Joined with Author Summary (avg_author_summary) using author_id
- Expanded avg_author_rating for each author
- Joined with Book Review Statistics (book_review_stats) using book_id
- Expanded review-related metrics: avg_review_words, min_review_words, max_review_words
- Removed unnecessary columns such as review_length and date_added
- Converted all review_text entries to lowercase for consistency
- Removed duplicate records based on review_id, book_id, and author_id to ensure uniqueness
- Renamed avg_rating to avg_book_rating for clarity

Output
- Columns: 17 total
- Data Quality: No duplicates, standardized text, complete
- Null values: 0%

All M (Power Query) code for this Fabric data pipeline has been provided in the folder: **`SQL_Script/02_Fabrics_SqlSteps.m`**

### Part 3 – Data Cleaning and Aggregation: Using DataBricks from Azure

Since the Fabric pipeline could not be published, the same cleaning and transformation steps were performed in Databricks. This involved inspecting and correcting column data types, converting the date_added column to ISO format (date_added_iso), and removing invalid or null rows. Duplicates were removed, and text columns were normalized by lowering review_text, removing malformed characters, capitalizing each word in title and name for potential NER tasks, and trimming leading/trailing spaces. Reviews with very short content were filtered using a review_length column, and future or invalid dates were removed.And,Aggregated features were created (avg_book_rating, num_reviews_per_book, author_avg_rating, min/max/avg_review_words). Additionally helper columns like review_length, date_added and review_word_count were removed. Finally, the cleaned and enriched dataset was saved to the gold folder as `gold.features_v1` for further analysis.
The implementation can be reviewed in the Notebooks folder: **`Notebooks/03_Databricks_Cleaned_Aggregated_Gold_Table.ipynb`**.

