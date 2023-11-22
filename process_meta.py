import json
from nltk.stem import PorterStemmer
import re
from nltk.corpus import words
import numpy as np
import pickle

# Path to your JSON file
file_path = 'data/meta_Magazine_Subscriptions.json'

# Lists to store the extracted data
also_buy = []
all_categories = []
porter_stemmer = PorterStemmer()
english_words = set(words.words())

# Read the file line by line
with open(file_path, 'r') as file:
    for line in file:
        # Parse the line as JSON
        data = json.loads(line)
        print(data)

        # Extract and append the needed data
        also_buy.append(data.get('also_buy', []))
        salesrank = data.get('salesRank', {})
        print(salesrank)
        exit()
        all_categories.append(next(iter(), ''))


categories = []
with open('data/meta_transactions.txt', 'w') as f:
    for text,category in zip(also_buy, all_categories):
        line = ",".join(text) + '\n'
        f.write(line)
        categories.append(category)

# Initialize an empty dictionary and list
category_to_number = {}
number_list = []

# Iterate through the list of strings
for string in categories:
    # Check if the string is already in the dictionary
    if string not in category_to_number:
        # Assign a unique number and add it to the dictionary
        unique_number = len(category_to_number)
        category_to_number[string] = unique_number
    # Append the assigned number to the list
    number_list.append(category_to_number[string])

# Print the dictionary and the list of numbers
print("Category to Number Mapping:", category_to_number)
print("List of Numbers:", number_list)

with open('data/meta_labels.pkl', 'wb') as f:
    pickle.dump(np.array(number_list), f)
