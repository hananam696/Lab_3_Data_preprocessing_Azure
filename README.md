# Lab4-feature-extraction

### **LAB OVERVIEW**
This project builds a feature-rich dataset from Goodreads book reviews to enable modeling of user ratings. The pipeline extracts text-based features such as sentiment, TF-IDF, and BERT embeddings.

1.	Data Splitting
The dataset was divided into training, validation, and test sets to ensure the model is tested fairly and prevents data leaking. To ensure that the findings are consistently repeatable, PySpark's randomSplit() method was used with a fixed random seed.

I did two splits where: First, 70% of the data was set aside for training. The remaining 30% was kept for further splitting. That 30% was then divided in half—half became validation data, the other half became the testing set.

After splitting is done verification was done by counting the rows to ensure that no data was lost, after confirming it was successful, each split was saved to the Gold layer in separate folders

**Reference Code:** See the implementation in **`Notebooks/01_data_splitting.ipynb`**

2.	Preprocessing review_text column
Before doing more feature extraction, made sure the review_text column is cleaned, so it was converted to lowercase, trimmed the spaces, emojis, URLs, and numbers were removed (placeholders kept), punctuation and extra spaces were removed, and very small reviews were filtered out. Additionally, two new text-based columns were created: review_length_words, which counts the number of words in each review, and review_length_chars, which counts the number of characters.

3. Feature Extraction
-  Sentiment feature:
 VADER library was used to extract sentiment components from the review text. For each review, extracted four scores: positive, negative, neutral, and compound which returned a scores Since the dataset is large,  pandas with PySpark UDFs was used to efficiently process all reviews in parallel. These sentiment features were then added as separate columns to the dataset for modeling.
