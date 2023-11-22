# Import the standard toolkit...
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from nltk.stem import PorterStemmer
from nltk.corpus import words
import re
import os
import pickle

porter_stemmer = PorterStemmer()
english_words = set(words.words())


def get_unique_stemmed_words(text, dictionary):
    text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
    article_words = re.findall(r'\w+', text.lower())  # convert to lowercase and split
    unique_words = set(article_words)  # get unique words
    stemmed_unique_words = {porter_stemmer.stem(word) for word in unique_words}

    filtered_words = stemmed_unique_words.intersection(dictionary)

    return list(filtered_words)


def process_folder(folder_path, output_file):
    count = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                processed_content = get_unique_stemmed_words(content, english_words)
                if len(processed_content)==0:
                    continue
                line = ",".join(processed_content) + '\n'
                output_file.write(line)
                print(content)
                count += 1
                if count==5:
                    break

    return count


def main():
    train_folder = 'data/aclImdb/train'
    output_file_path = "data/imdb_transactions_2.txt"
    labels = []

    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        number_pos = process_folder(os.path.join(train_folder, "pos"), output_file)
        number_neg = process_folder(os.path.join(train_folder, "neg"), output_file)

    labels = [1] * number_pos + [0] * number_neg

    # with open('data/imdb_labels.pkl', 'wb') as f:
    #     pickle.dump(np.array(labels), f)


if __name__ == "__main__":
    main()

