import os
import dask.distributed
import pandas as pd
from nltk.corpus import stopwords, words
import dask.bag as db
from collections import defaultdict
import numpy as np
import pickle
import datetime
import dask.dataframe as dd
import json
import itertools


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
    print(f"Frequent Itemsets Level {k} completed")
    print(f"{len(freq_new_level)} itemsets found")

    # iteratively mine for frequent itemsets
    while True:
        k += 1
        
        prev_freq = list(freq_new_level.keys())
        candidates = generate_candidate_itemsets(prev_freq)
        #pruned_candidates = prune_candidates(candidates, prev_freq)

        # print(candidates)

        freq_new_level = get_frequent_item_sets(data_file, candidates, min_support, blocksize)

        # if no new frequent itemsets at this level, stop mining
        if not freq_new_level:
            print(f"No more frequent itemsets to be found at level {k}!")
            break

        # add new frequent itemsets to overall set
        freq_itemsets.update(freq_new_level)
        print(f"Frequent Itemsets Level {k} completed")
        print(f"{len(freq_new_level)} itemsets found")

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
    print('test')

    text_ddf = dd.read_csv(data_file, sep='|', header=None, names=['line'], blocksize=blocksize)
    # text = db.read_text(data_file, blocksize=blocksize)
    empty = defaultdict(int)
    meta = pd.DataFrame({'output': [empty]})

    print(text_ddf.npartitions)

    print('test2')
    print(meta)

    item_sets_json = json.dumps(list(item_sets), default=serialize_frozenset)
    item_sets_future = client.scatter(item_sets_json, broadcast=True)
    print('scattered')
    results = text_ddf.map_partitions(count_itemsets_in_partition, item_sets_future, meta=meta).compute()
    print('test3')
    output_dict = defaultdict(int)
    for d in results['output']:
        output_dict = combine_counts(output_dict, d)

    filtered_item_sets = {frozenset(key): value for key, value in output_dict.items() if value > min_support}

    return filtered_item_sets


def count_itemsets_in_partition(partition, item_sets_json):
    item_sets = [frozenset(lst) for lst in json.loads(item_sets_json)]
    item_count = defaultdict(int)

    for line in partition['line']:
        line_set = set(line.split(','))
        for item in item_sets:
            if item.issubset(line_set):
                item_count[item] += 1  # in here, item is a frozenset

    output = pd.DataFrame({'output':[item_count]})

    return output


def combine_counts(count1, count2):
    for key, value in count2.items():
        count1[key] += value
    del count2
    return count1


# def generate_candidate_itemsets(freq_itemsets):
#     candidate_itemsets = set()
#     for itemset1 in freq_itemsets:
#         for itemset2 in freq_itemsets:
#             if itemset1 != itemset2:
#                 new_candidate = itemset1.union(itemset2)
#                 if len(new_candidate) == len(itemset1) + 1:
#                     candidate_itemsets.add(new_candidate)
#     return candidate_itemsets

## DASK VERSION OF GENERATE CANDIDATES
def valid_candidate(pairs, itemsets_json):
    freq_itemsets = [frozenset(lst) for lst in json.loads(itemsets_json)]
    new_candidates = set()

    for pair in pairs:
        itemset1, itemset2 = pair
        new_candidate = itemset1.union(itemset2)
        if len(new_candidate) == len(itemset1) + 1:
            if all(frozenset(subset) in freq_itemsets for subset in
                   itertools.combinations(new_candidate, len(new_candidate) - 1)):
                new_candidates.add(new_candidate)
    return new_candidates


def generate_candidate_itemsets(freq_itemsets, num_sublists=6):
    print('generating candidates')
    itemsets_list = list(freq_itemsets)
    total_pairs = [(itemsets_list[i], itemsets_list[j]) for i in range(len(itemsets_list)) for j in
                   range(i + 1, len(itemsets_list))]

    item_sets_json = json.dumps(itemsets_list, default=serialize_frozenset)
    item_sets_future = client.scatter(item_sets_json, broadcast=True)

    # Split the list into sublists
    sublist_size = len(total_pairs) // num_sublists
    sublists = [total_pairs[i:i + sublist_size] for i in range(0, len(total_pairs), sublist_size)]
    print("total_pairs size: ", str(total_pairs.__sizeof__()))

    print('pre-map')
    futures = client.map(valid_candidate, sublists, itemsets_json=item_sets_future)
    print('map')
    # futures = []
    # for sublist in sublists:
    #     future = client.submit(valid_candidate, sublist, freq_itemsets)
    #     futures.append(future)

    # print('map')
    candidate_itemsets = set()
    for future in dask.distributed.as_completed(futures):
        print('a')
        candidate_itemsets.update(future.result())
        print('b')

    # Create a masterlist of Dask bags
    # masterlist = db.from_sequence(total_pairs, npartitions=6)
    #
    # item_sets_json = json.dumps(itemsets_list, default=serialize_frozenset)
    # item_sets_future = client.scatter(item_sets_json, broadcast=True)
    #
    # # Process each sublist in parallel
    # candidate_itemsets = masterlist.map_partitions(valid_candidate, item_sets_future).compute()

    # print('post-map')
    #     #candidate_itemsets.update(batch_candidates)
    # #for batch in batch_candidates:
    # #    candidate_itemsets.update(batch)
    #
    #
    return candidate_itemsets
## END DASK VERSION OF GENERATE CANDIDATES

# def generate_candidate_itemsets(freq_itemsets):
#     candidate_itemsets = set()
#
#     for i in range(len(freq_itemsets)):
#         for j in range(i + 1, len(freq_itemsets)):
#             itemset1 = freq_itemsets[i]
#             itemset2 = freq_itemsets[j]
#
#             # Check if the first k-1 elements are the same
#             if len(itemset1.union(itemset2)) == len(itemset1) + 1:
#                 new_candidate = itemset1.union(itemset2)
#                 if all(frozenset(subset) in freq_itemsets for subset in
#                        itertools.combinations(new_candidate, len(new_candidate) - 1)):
#                     candidate_itemsets.add(new_candidate)
#
#     return candidate_itemsets


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


def serialize_frozenset(obj):
    """Custom serialization for frozenset objects."""
    if isinstance(obj, frozenset):
        return list(obj)  # Convert frozenset to list
    raise TypeError(f"Type {type(obj)} not serializable")


if __name__ == "__main__":
    count_dict = defaultdict(int)

    # noinspection PyBroadException
    try:
        client = dask.distributed.Client(n_workers=6, threads_per_worker=1)  # Adjust based on your CPU
        stopwords_set = set(stopwords.words('english'))
        # data = 'data/combined_stemmed.csv'
        data = 'data/pruned.csv'
        block_size = "1MB"

        frequent_itemsets = apriori_disk(data_file=data,
                                         exclude=stopwords_set,
                                         min_support_percent=.6,
                                         blocksize=block_size)

        freq_list = list(frequent_itemsets.keys())
        with open('data/frequent_itemsets.pkl', 'wb') as f:
            pickle.dump(frequent_itemsets, f)

        exit()

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

        # os.system('shutdown -s -t 0')
    except KeyboardInterrupt as e:
        exit()
    except Exception as e:
        with open('log.txt', 'w') as f:
            now = datetime.datetime.now()
            f.write(now.strftime("%Y-%m-%d %H:%M:%S"))
            f.write("\n")
            f.write(str(e))
        # os.system('shutdown -s -t 0')




# backup
# def get_frequent_item_sets(data_file, item_sets, min_support, blocksize):
#     text = db.read_text(data_file, blocksize=blocksize)
#
#     results = text.map(count_itemsets_in_line, item_sets).fold(combine_counts).compute()
#
#     print(f"size of results: {results.__sizeof__() / 1024}")
#
#     filtered_item_sets = {frozenset(key): value for key, value in results.items() if value > min_support}
#
#     return filtered_item_sets
#
#
# def count_itemsets_in_line(line, item_sets):
#     item_count = defaultdict(int)
#
#     line_set = set(line.split(','))
#     for item in item_sets:
#         if item.issubset(line_set):
#             item_count[item] += 1  # in here, item is a frozenset
#
#     return item_count