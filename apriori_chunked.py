import pandas as pd
import csv
import multiprocessing
import dask.bag as db
from dask.distributed import Client
from nltk.corpus import words
from nltk.corpus import stopwords
import numpy as np
from collections import defaultdict


def apriori_disk(data_file, min_support_percent):
    #item_sets = frozenset(set(words.words()))
    stop_words = set(stopwords.words('english'))

    with open(data_file, 'r') as f:
        row_count = sum(1 for row in f)
        min_support = int(row_count*min_support_percent)

    item_sets = {frozenset([item]) for item in words.words() if item not in stop_words}

    freq_itemsets = freq_new_level = get_frequent_item_sets_alt(data_file, item_sets, min_support)
    k = 1

    while True:
        if k>2: break
        k += 1
        candidates = generate_candidate_itemsets(freq_new_level)
        pruned_candidates = prune_candidates(candidates, freq_itemsets)

        freq_new_level = get_frequent_item_sets(data_file, pruned_candidates, min_support)

        if not freq_new_level:
            break

        freq_itemsets.update(freq_new_level)#{frozenset(items) for items in freq_new_level})
        print(f"Frequent Itemsets Level {k} completed")

    print(freq_itemsets)

    return


def count_itemsets_in_line(line, item_sets):
    item_count = defaultdict(int)

    line_set = set(line.split(','))
    for item in item_sets:
        if item.issubset(line_set):
            item_count[item] += 1 # in here, item is a frozenset

    # for item in line.split(','):
    #     if item in item_sets:
    #         item_count[item] += 1 #in here, item is a string
    #     if frozenset({'test'}).issubset(item_sets):
    #         line_set = set(line.split(','))
    #         continue
    return item_count


def count_itemsets_in_line_alt(line, item_sets):
    #made this alt function because dictionary has 235k items, but most are not used - iterating through all 235k items
    #for every line is prohibitively slow
    item_count = defaultdict(int)

    for item in line.split(','):
        item = frozenset({item})
        if item in item_sets:
            item_count[frozenset(item)] += 1 #in here, item is a string

    return item_count


def get_frequent_item_sets_alt(data_file, item_sets, min_support):

    text = db.read_text(data_file, blocksize="100MB")

    results = text.map(count_itemsets_in_line_alt, item_sets).fold(combine_counts).compute()

    filtered_item_sets = {frozenset(key): value for key, value in results.items() if value > min_support}

    return filtered_item_sets



def get_frequent_item_sets(data_file, item_sets, min_support):

    text = db.read_text(data_file, blocksize="100MB")

    results = text.map(count_itemsets_in_line, item_sets).fold(combine_counts).compute()

    filtered_item_sets = {frozenset(key): value for key, value in results.items() if value > min_support}

    return filtered_item_sets


def generate_candidate_itemsets(freq_itemsets):
    candidate_itemsets = set()
    for itemset1 in freq_itemsets:
        for itemset2 in freq_itemsets:
            if itemset1 != itemset2:
                new_candidate = itemset1.union(itemset2)
                if len(new_candidate) == len(itemset1) + 1:
                    candidate_itemsets.add(new_candidate)
    return candidate_itemsets


def prune_candidates(candidates, freq_itemsets):
    pruned_candidates = set()
    for candidate in candidates:
        all_subsets_are_frequent = True
        for item in candidate:
            subset = candidate - {item}
            if subset not in freq_itemsets:
                all_subsets_are_frequent = False
                break
        if all_subsets_are_frequent:
            pruned_candidates.add(candidate)
    return pruned_candidates


def combine_counts(count1, count2):
    for key,value in count2.items():
        count1[key] += value
    return count1


# def csv_dask(data_file):
#     # Read the file as text
#     text = db.read_text(data_file)
#
#     # Process each line and flatten the results
#     result = text.map(process_line).fold(set.union, initial=set()).compute()
#
#     # Combine the results into one set
#     combined_set = set(result)
#
#     return combined_set


def load_data_chunked(filename, chunk_size):
    # Load data from a CSV file in chunks.
    data_chunks = pd.read_csv(filename, chunksize=chunk_size)
    return data_chunks


def process_line(line):
    return set(line.strip().split(","))


def read_csv_dask(data_file):
    # Read the file as text
    text = db.read_text(data_file, blocksize='100MB')

    # Process each line and flatten the results
    result = text.map(process_line).fold(set.union, initial=set()).compute()

    # Combine the results into one set
    combined_set = set(result)

    return combined_set


def read_csv(data_file):
    items_set = set()
    with open(data_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for i, transaction in enumerate(reader):
            items_set.update(transaction)

    return items_set


def candidate_generation(itemSet, length):
    """
    Generate all possible combinations of a specific length
    """
    tempSet = set()
    for i in itemSet:
        for j in itemSet:
            if len(i.union(j)) == length:
                tempSet.add(i.union(j))
    return tempSet


def apriori_chunked(data_chunks, min_support):
    candidate_set = set()
    total_transactions = 0
    frequent_itemsets = set()
    k = 1

    for chunk in data_chunks:
        total_transactions += len(chunk)
        for index, row in chunk.iterrows():
            transaction = set(row.dropna())  # Convert the row to a set of items.
            for item in transaction:
                candidate_set.add(frozenset([item]))  # Each item is a candidate by itself.

        frequent_itemsets_k = set()
        for candidate in candidate_set:
            count = sum(1 for chunk in data_chunks if candidate.issubset(chunk))
            support = count / total_transactions
            if support >= min_support:
                frequent_itemsets_k.add(candidate)

        frequent_itemsets.update(frequent_itemsets_k)

        candidate_set = candidate_generation(frequent_itemsets_k, k)
        k += 1

    return frequent_itemsets


if __name__ == "__main__":
    client = Client(n_workers=6, threads_per_worker=2)  # Adjust based on your CPU

    #apriori_disk('data/combined.csv', 1)
    #apriori_disk('data/output_5.csv',0.3)
    apriori_disk('data/articles_items.csv_worker_0.csv',0.5)