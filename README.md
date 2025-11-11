# Lab4-feature-extraction

### **LAB OVERVIEW**

This project builds a feature-rich dataset from Goodreads book reviews to enable modeling of user ratings. The pipeline extracts text-based features such as sentiment, TF-IDF, and BERT embeddings.

1.	Data Splitting

The dataset was divided into training, validation, and test sets to ensure the model is tested fairly and prevents data leaking. To ensure that the findings are consistently repeatable, PySpark's randomSplit() method was used with a fixed random seed.

I did two splits where: First, 70% of the data was set aside for training. The remaining 30% was kept for further splitting. That 30% was then divided in half—half became validation data, the other half became the testing set.

After splitting is done verification was done by counting the rows to ensure that no data was lost, after confirming it was successful, each split was saved to the Gold layer in separate folders

**Reference Code:** See the implementation in **`Notebooks/01_data_splitting.ipynb`**

2.	Preprocessing review_text column

Before doing more feature extraction, made sure the review_text column is cleaned, so it was converted to lowercase, trimmed the spaces, emojis, URLs, and numbers were removed (placeholders kept), punctuation and extra spaces were removed, and very small reviews were filtered outusing the extract `review_length_chars` coloumn that counts the total number of characters.

3. Feature Extraction

- Basic Text Features:
Basic text features were created to capture the length of each review. review_length_words counts the total number of words in a review, while review_length_chars counts the total number of characters. These features provide simple numerical information about the text

-  Sentiment feature:
**On full Data**
The VADER library was used to extract sentiment components from the review text on the full dataset. For each review, four scores were extracted: positive, negative, neutral, and compound. Since the dataset is large, Pandas UDFs with PySpark were used to efficiently process all reviews in parallel. These sentiment features were then added as separate columns to the dataset for use in feature extraction.

- TF-IDF:
**On Sample Data** - for testing
A scikit-learn TfidfVectorizer was used to get TF-IDF features from the reviews. A sample of 100,000 reviews was used to fit the vectorizer. It considered the top 500 words, removed common English stop words, and included both single words and word pairs. Pandas UDFs were then used to calculate summary features (tfidf_mean, tfidf_max, tfidf_min) for each review. These features were added as separate columns to the dataset.

**On Full Data**
For the full dataset, PySpark ML was used to compute TF-IDF features. Reviews were split into words, converted to term frequencies, and then transformed into TF-IDF vectors. These vectors were stored in a column called tfidf_features and added back to the DataFrame. Spark’s distributed processing made it possible to handle the entire dataset efficiently.

- Sentence-Bert:
**On Sample Data** - for testing
Sentence-BERT (model name: all-MiniLM-L6-v2)was used  for Semantic Embedding Features because it is fast and efficient. It vectorized the entire review into a 384-dimensional dense embedding capturing rich semantic relationships beyond simple word counts or TF-IDF. Since the dataset was large, Principal Component Analysis (PCA) was applied to reduce the embedding dimensionality (from 384 to 128) for improved computational efficiency and noise reduction. Additionally, the code was run on batches  to handle large-scale data efficiently.

**On Full Data**
The same Sentence-BERT model was used to the full dataset. Also , here PCA wasn't used to reduce the size of embeddings. However, to handle the large dataset, the reviews were processed in batches using Spark’s Pandas UDF feature, which allowed to efficiently create embeddings without running out of memory.

4. Combined Feature Set and Output

After extracting new features, basic text features such as review_length_words and review_length_chars, sentiment score features like (sentiment_pos, sentiment_neg, sentiment_neu, sentiment_compound), and semantic embeddings from Sentence-BERT (sbert_features) and TF-IDF vectors (tfidf_features) were merged with metadata columns (review_id, book_id, rating) to create a complete feature matrix. The final dataset was saved to the Gold layer in a new folder named features_v3 as a Delta table using overwrite mode, ensuring no schema conflicts and making it ready for downstream predictive modeling.