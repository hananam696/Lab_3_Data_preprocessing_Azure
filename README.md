# Lab4-feature-extraction

### **LAB OVERVIEW**

This project builds a feature-rich dataset from Goodreads book reviews to enable modeling of user ratings. The pipeline extracts text-based features such as sentiment, TF-IDF, and BERT embeddings.

1.	Data Splitting

The dataset was divided into training, validation, and test sets to ensure the model is tested fairly and prevents data leaking. To ensure that the findings are consistently repeatable, PySpark's randomSplit() method was used with a fixed random seed.

I did two splits where: First, 70% of the data was set aside for training. The remaining 30% was kept for further splitting. That 30% was then divided in half—half became validation data, the other half became the testing set.

After splitting is done verification was done by counting the rows to ensure that no data was lost, after confirming it was successful, each split was saved to the Gold layer in separate folders

**Reference Code:** See the implementation in **`jupyter_notebook/01_data_splitting.ipynb`**

2.	Preprocessing review_text column

Before doing more feature extraction, made sure the review_text column is cleaned, so it was converted to lowercase, trimmed the spaces, emojis, URLs, and numbers were removed (placeholders kept), punctuation and extra spaces were removed, and very small reviews were filtered outusing the extract `review_length_chars` coloumn that counts the total number of characters.

3. Feature Extraction

- Basic Text Features:

Basic text features were created to capture the length of each review. review_length_words counts the total number of words in a review, while review_length_chars counts the total number of characters. These features provide simple numerical information about the text

-  Sentiment feature:

The VADER library was used to extract sentiment scores from the review text for the full dataset. For each review, four scores were calculated: positive, negative, neutral, and compound. Empty or invalid reviews were assigned a score of 0.0 for safety, although empty rows had already been handled during data cleaning. Spark UDFs were used to efficiently compute the scores for all reviews in parallel. These sentiment features capture the emotional tone of each review and were added as separate columns to the dataset. The resulting sentiment DataFrame (sentiment_train) was saved as a Delta table

- TF-IDF:

TF‑IDF features were generated for the full reviews dataset using a Spark ML pipeline. Each review was tokenized into individual words, and common stopwords (such as 'the' and 'and') were removed because they do not add significant semantic value. The remaining words were first converted into term‑frequency vectors using CountVectorizer, which counts how often each word appears in a review, and then transformed into TF‑IDF representations that give higher weights to words that are frequent in a given review but relatively rare across all reviews in the dataset. This turns the raw text into numerical feature vectors. The resulting TF‑IDF feature DataFrame (tfidf_train) was saved as a Delta table.

- Additional Feature - (Review Readability feature):

A readability score was added to the dataset to show how easy each review is to read, using the Flesch Reading Ease formula, which measures text difficulty based on average sentence length and the number of syllables in each word. A Spark function calculates this score for every review, and if a review is empty or cannot be processed, the score is set to 0. This readability score helps describe the writing style of each review and is stored as a new column for later analysis or model training.

- Sentence-Bert:

Semantic embeddings were created for the full dataset to capture the meaning of each review. This was done using the SentenceTransformer model (all-MiniLM-L6-v2), which converts text into dense numerical vectors that represent the semantic relationships between words and sentences. A Spark UDF was applied to generate an embedding for each review. Empty or invalid text entries were skipped for safety, even though the text had already been cleaned earlier. These embeddings help identify how similar or different reviews are in terms of meaning, allowing models to understand context more effectively. The resulting semantic embedding DataFrame (embedded_train) was saved as a Delta table.

4. Combined Feature Set and Output

All the extracted features, including sentiment scores, TF‑IDF vectors, readability scores, and semantic embeddings from SBERT, were first saved as separate Delta tables. In the final step, these features were consolidated by loading the embedded_train table, which already contained all processed training features. The complete set of features was then saved as a single Delta table called combined_train. This merged dataset was verified and stored in the Gold layer at feature_v2/combined_train, making it ready for modeling.


**Reference Code:** See the implementation in **`jupyter_notebook/02_goodreads_feature_extraction.ipynb`**