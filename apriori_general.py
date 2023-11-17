import os
import dask.distributed
from nltk.corpus import stopwords, words
import dask.bag as db
from collections import defaultdict
import numpy as np
import pickle
import datetime

# main function to execute disk-based apriori algorithm
# data_file: text file, where each line is a transaction and each item is comma-separated
# exclude: set of items (strings) to exclude
# min_support_percent: from 0 to 1, what % of support is required
# blocksize: text file will be broken into chunks to process, this determines size of chunk
def apriori_disk(data_file, exclude, min_support_percent, blocksize):

    # count the number of rows needed to fulfil the min_support_percent
    with open(data_file, 'r') as f:
        row_count = sum(1 for row in f)
        min_support = int(row_count * min_support_percent)
        print("no rows: ", row_count)

    # do the first pass of the dataset to obtain the first set of frequent itemsets
    # overall set will be stored in freq_itemsets, freq_new_level only temporary
    freq_itemsets = freq_new_level = get_frequent_item_sets_first_pass(data_file, exclude, min_support, blocksize)
    k = 1  # current length of itemsets to check for

    # iteratively mine for frequent itemsets
    while True:
        k += 1

        candidates = generate_candidate_itemsets(freq_new_level)
        pruned_candidates = prune_candidates(candidates, freq_itemsets)

        freq_new_level = get_frequent_item_sets(data_file, pruned_candidates, min_support, blocksize)

        # if no new frequent itemsets at this level, stop mining
        if not freq_new_level:
            break

        # add new frequent itemsets to overall set
        freq_itemsets.update(freq_new_level)
        print(f"Frequent Itemsets Level {k} completed")

    return freq_itemsets


# uses alternate function to count itemsets to account for stuff to exclude
def get_frequent_item_sets_first_pass(data_file, exclude, min_support, blocksize):
    text = db.read_text(data_file, blocksize=blocksize)

    results = text.map(count_itemsets_in_line_first_pass, exclude).fold(combine_counts).compute()

    filtered_item_sets = {frozenset(key): value for key, value in results.items() if value > min_support}

    return filtered_item_sets

# counts the itemsets present in the given line
def count_itemsets_in_line_first_pass(line, exclude):
    item_count = defaultdict(int)

    for item in line.split(','):
        item = frozenset({item})
        if item not in exclude:
            item_count[item] += 1  # in here, item is a string
    return item_count


def get_frequent_item_sets(data_file, item_sets, min_support, blocksize):
    text = db.read_text(data_file, blocksize=blocksize)

    results = text.map(count_itemsets_in_line, item_sets).fold(combine_counts).compute()

    filtered_item_sets = {frozenset(key): value for key, value in results.items() if value > min_support}

    return filtered_item_sets


def combine_counts(count1, count2):
    for key, value in count2.items():
        count1[key] += value
    return count1


def count_itemsets_in_line(line, item_sets):
    item_count = defaultdict(int)

    line_set = set(line.split(','))
    for item in item_sets:
        if item.issubset(line_set):
            item_count[item] += 1  # in here, item is a frozenset

    return item_count


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


# used after all frequent itemsets have been obtained, find which frequent itemsets each line possesses
# and save into a vector - all vectors stored in an overall list
def check_itemsets(data_file, itemsets, blocksize):
    text = db.read_text(data_file, blocksize=blocksize)

    results = text.map(check_itemsets_in_line, itemsets).compute()

    return results


def check_itemsets_in_line(line, itemsets):
    line_set = set(line.split(','))
    itemsets_present = np.zeros(len(itemsets))

    for i, itemset in enumerate(itemsets):
        if itemset.issubset(line_set):
            itemsets_present[i] = 1

    return itemsets_present


if __name__ == "__main__":
    # noinspection PyBroadException
    try:
        client = dask.distributed.Client(n_workers=6, threads_per_worker=1)  # Adjust based on your CPU
        stopwords_set = set(stopwords.words('english'))

        #apriori_disk('data/combined.csv', 1)
        #apriori_disk('data/output_5.csv',0.3)
        frequent_itemsets = apriori_disk('data/combined_stemmed.csv', stopwords_set, 0.4, "30MB")
        freq_list = list(frequent_itemsets.keys())
        with open('data/frequent_itemsets.pkl', 'wb') as f:
            pickle.dump(frequent_itemsets, f)

        #worried that this variable is actually too big, maybe I need to save it to .txt line by line instead
        itemset_features = check_itemsets('data/combined_stemmed.csv', freq_list, "30MB")
        #print(itemset_features)
        #print(type(itemset_features))
        #print(itemset_features.__sizeof__())
        with open('data/itemset_features.pkl', 'wb') as f:
            pickle.dump(itemset_features, f)

        with open('log.txt', 'w') as f:
            now = datetime.datetime.now()
            f.write(now.strftime("%Y-%m-%d %H:%M:%S"))

        os.system('shutdown -s -t 0')
    except KeyboardInterrupt as e:
        exit()
    except Exception as e:
        with open('log.txt', 'w') as f:
            now = datetime.datetime.now()
            f.write(now.strftime("%Y-%m-%d %H:%M:%S"))
            f.write("\n")
            f.write(str(e))
        os.system('shutdown -s -t 0')

