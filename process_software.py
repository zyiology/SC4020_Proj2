import json
from nltk.stem import PorterStemmer
import re
from nltk.corpus import words
import numpy as np
import pickle

# Path to your JSON file
file_path = 'data/Appliances_5.json'

# Lists to store the extracted data
review_texts = []
overall_ratings = []
porter_stemmer = PorterStemmer()
english_words = set(words.words())

# Read the file line by line
with open(file_path, 'r') as file:
    for line in file:
        # Parse the line as JSON
        data = json.loads(line)

        # Extract and append the needed data
        review_texts.append(data.get('reviewText', ''))
        overall_ratings.append(data.get('overall', 0))


def get_unique_stemmed_words(text, dictionary):
    text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
    article_words = re.findall(r'\w+', text.lower())  # convert to lowercase and split
    unique_words = set(article_words)  # get unique words
    stemmed_unique_words = {porter_stemmer.stem(word) for word in unique_words}

    filtered_words = stemmed_unique_words.intersection(dictionary)

    return list(filtered_words)

ratings = []
with open('data/appliances_reviews_transactions.txt', 'w') as f:
    for text,rating in zip(review_texts,overall_ratings):
        words = get_unique_stemmed_words(text, english_words)
        if len(words)==0:
            continue
        line = ",".join(words) + '\n'
        f.write(line)
        ratings.append(rating)

with open('data/appliances_reviews_labels.pkl', 'wb') as f:
    pickle.dump(np.array(ratings), f)

print(len(ratings))
