# Import the standard toolkit...
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from nltk.stem import PorterStemmer
import re

from sklearn.datasets import fetch_20newsgroups
raw_posts = fetch_20newsgroups(remove=('headers', 'footers', 'quotes'))
porter_stemmer = PorterStemmer()



def get_unique_stemmed_words(text, dictionary):
    text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
    article_words = re.findall(r'\w+', text.lower())  # convert to lowercase and split
    unique_words = set(article_words)  # get unique words
    stemmed_unique_words = {porter_stemmer.stem(word) for word in unique_words}

    filtered_words = stemmed_unique_words.intersection(dictionary)

    return list(filtered_words)

with open('data/documents.txt', 'w') as f:
    for rawtext in raw_posts.data:
        print(rawtext)
        exit()

