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
from nltk.stem import PorterStemmer
import traceback



# main function to execute disk-based apriori algorithm
# data_file: text file, where each line is a transaction and each item is comma-separated
# exclude: set of items (strings) to exclude
# min_support_percent: from 0 to 1, what % of support is required
# blocksize: text file will be broken into chunks to process, this determines size of chunk

#should accept a client too!!!
def apriori_disk(data_file, exclude, min_support_percent, blocksize):

    # count the number of rows needed to fulfil the min_support_percent
    with open(data_file, 'r') as f:
        row_count = sum(1 for row in f)
        min_support = int(row_count * min_support_percent)
        print("no rows: ", row_count)

    # do the first pass of the dataset to obtain the first set of frequent itemsets
    # overall set will be stored in freq_itemsets, freq_new_level only temporary
    string_to_integer, freq_itemsets = get_frequent_item_sets_first_pass(data_file, exclude, min_support, blocksize)
    print('First pass completed')

    if not string_to_integer or not freq_itemsets:
        print('No frequent itemsets found')
        return

    string_to_integer_future = client.scatter(string_to_integer, broadcast=True)
    freq_new_level = freq_itemsets.copy()
    k = 1  # current length of itemsets to check for
    print(f"Frequent Itemsets Level {k} completed")
    print(f"{len(freq_new_level)} itemsets found")

    # iteratively mine for frequent itemsets
    while True:
        k += 1
        
        prev_freq = list(freq_new_level.keys())

        print('generating candidates')
        candidates = generate_candidate_itemsets(prev_freq)
        print(f'{len(candidates)} candidates generated')

        #pruned_candidates = prune_candidates(candidates, prev_freq)

        if not candidates:
            print(f"No more valid candidates to be found at level {k}!")
            break

        # print(candidates)

        print("checking frequency of itemsets...")
        freq_new_level = get_frequent_item_sets(data_file, candidates, string_to_integer_future, min_support, blocksize)

        # if no new frequent itemsets at this level, stop mining
        if not freq_new_level:
            print(f"No more frequent itemsets to be found at level {k}!")
            break

        # add new frequent itemsets to overall set
        freq_itemsets.update(freq_new_level)
        print(f"Frequent Itemsets Level {k} completed")
        print(f"{len(freq_new_level)} itemsets found\n")

    return freq_itemsets, string_to_integer


# uses alternate function to count itemsets to account for stuff to exclude
def get_frequent_item_sets_first_pass(data_file, exclude, min_support, blocksize):
    text = db.read_text(data_file, blocksize=blocksize)

    results = text.map(count_itemsets_in_line_first_pass, exclude).fold(combine_counts).compute()

    string_to_integer = defaultdict(int)  # Dictionary to map strings to unique integers
    integer_to_values = defaultdict(int) # Dictionary to map integers to frozensets of values

    current_integer = 0  # Start with an integer counter

    for key, value in results.items():
        if value > min_support:
            # Check if the key is already mapped to an integer, if not, assign a new integer
            if key not in string_to_integer:
                string_to_integer[key] = current_integer
                current_integer += 1

            # Get the integer associated with the key
            integer_key = string_to_integer[key]

            # Check if the integer is already in the integer_to_values dictionary
            if integer_key not in integer_to_values:
                integer_to_values[frozenset({integer_key})] = value

    #filtered_item_sets = {key: value for key, value in results.items() if value > min_support}

    return string_to_integer, integer_to_values

# counts the itemsets present in the given line
def count_itemsets_in_line_first_pass(line, exclude):
    item_count = defaultdict(int)

    for item in line.split(','):

        if item not in exclude:
            item_count[item] += 1  # in here, item is a string
    return item_count


def get_frequent_item_sets(data_file, item_sets, string_to_integer, min_support, blocksize):

    text_ddf = dd.read_csv(data_file, sep='|', header=None, names=['line'], blocksize=blocksize)
    # text = db.read_text(data_file, blocksize=blocksize)
    empty = defaultdict(int)
    meta = pd.DataFrame({'output': [empty]})

    item_sets_bytes = pickle.dumps(item_sets) #json.dumps(list(item_sets), default=serialize_frozenset)
    item_sets_future = client.scatter(item_sets_bytes, broadcast=True)

    results = text_ddf.map_partitions(count_itemsets_in_partition, item_sets_future, string_to_integer, meta=meta).compute()
    output_dict = defaultdict(int)
    for d in results['output']:
        output_dict = combine_counts(output_dict, d)

    filtered_item_sets = {key: value for key, value in output_dict.items() if value > min_support}

    del item_sets_future

    return filtered_item_sets


def count_itemsets_in_partition(partition, item_sets_bytes, string_to_integer):
    item_sets = pickle.loads(item_sets_bytes)
    #item_sets = [frozenset(lst) for lst in json.loads(item_sets)]
    item_count = defaultdict(int)

    for line in partition['line']:
        line_set = set(string_to_integer[element] for element in line.split(',') if element in string_to_integer.keys())
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
def valid_candidate(pairs, itemsets_set_bytes):
    #freq_itemsets = [frozenset(lst) for lst in json.loads(itemsets_json)]
    itemsets_set = pickle.loads(itemsets_set_bytes)
    new_candidates = set()

    for pair in pairs:
        itemset1, itemset2 = pair
        new_candidate = itemset1.union(itemset2)

        if len(new_candidate) == len(itemset1) + 1:
            # Check if every subset formed by removing one item is frequent
            if all(frozenset(new_candidate - {item}) in itemsets_set for item in new_candidate):
                new_candidates.add(frozenset(new_candidate))

    return new_candidates

    # for pair in pairs:
    #     itemset1, itemset2 = pair
    #     new_candidate = itemset1.union(itemset2)
    #     if len(new_candidate) == len(itemset1) + 1:
    #         if all(frozenset(subset) in freq_itemsets for subset in
    #                itertools.combinations(new_candidate, len(new_candidate) - 1)):
    #             new_candidates.add(new_candidate)
    # return new_candidates


def generate_candidate_itemsets(itemsets_list:list, num_sublists=6):
    total_pairs = [(itemsets_list[i], itemsets_list[j]) for i in range(len(itemsets_list)) for j in
                   range(i + 1, len(itemsets_list))]

    print(f"size of total pairs: {total_pairs.__sizeof__()/1024}KB")

    # use dask if enough items
    if len(total_pairs)>50:

        # item_sets_json = json.dumps(itemsets_list, default=serialize_frozenset)
        # item_sets_future = client.scatter(item_sets_json, broadcast=True)

        itemsets_set_bytes = pickle.dumps(set(itemsets_list))  # json.dumps(list(item_sets), default=serialize_frozenset)
        itemsets_set_future = client.scatter(itemsets_set_bytes, broadcast=True)

        # Split the list into sublists
        sublist_size = len(total_pairs) // num_sublists
        sublists = [total_pairs[i:i + sublist_size] for i in range(0, len(total_pairs), sublist_size+1)]

        #futures = client.map(valid_candidate, sublists, itemsets_bytes=item_sets_future)
        # candidate_itemsets = set()
        # for future in dask.distributed.as_completed(futures):
        #     candidate_itemsets.update(future.result())

        sublists_future = client.scatter(sublists)
        print('scattered sublists')

        futures = []
        for sublist_future in sublists_future:
            #future = client.submit(valid_candidate, sublist, freq_itemsets)
            future = dask.delayed(valid_candidate)(sublist_future, itemsets_set_future)
            futures.append(future)

        results = dask.compute(*futures)
        candidate_itemsets = set()
        for result in results:
            candidate_itemsets.update(result)

        return list(candidate_itemsets)
    else:
        return list(valid_candidate(total_pairs, pickle.dumps(itemsets_list)))# json.dumps(itemsets_list, default=serialize_frozenset)))

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

## END DASK VERSION OF GENERATE CANDIDATES

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
def check_itemsets(data_file, itemsets, string_to_integer, blocksize):
    bag = db.read_text(data_file, blocksize=blocksize)

    delayed = bag.to_delayed()

    string_to_integer_future = client.scatter(pickle.dumps(string_to_integer), broadcast=True)
    itemsets_future = client.scatter(pickle.dumps(itemsets), broadcast=True)

    futures = []
    for partition in delayed:
        future = dask.delayed(check_itemsets_in_line_partition)(partition, string_to_integer_future, itemsets_future)
        futures.append(future)

    results = dask.compute(*futures)
    #results = text.map(check_itemsets_in_line, string_to_integer_future, itemsets_future).compute()
    # results = text.map_partitions(check_itemsets_in_line_partition, string_to_integer_future, itemsets_future).compute()

    results_stacked = np.vstack(results)

    return results_stacked


def check_itemsets_in_line_partition(lines, string_to_integer_byte, itemsets_byte):
    string_to_integer = pickle.loads(string_to_integer_byte)
    itemsets = pickle.loads(itemsets_byte)
    present_list = np.zeros((len(lines), len(itemsets)), dtype=bool)
    for i, line in enumerate(lines):
        line_set = set(string_to_integer[element] for element in line.split(',') if element in string_to_integer.keys())
        #itemsets_present = np.zeros(len(itemsets))

        for j, itemset in enumerate(itemsets):
            if itemset.issubset(line_set):
                present_list[i, j] = 1

    return present_list


def check_itemsets_in_line(line, string_to_integer, itemsets):
    line_set = set(string_to_integer[element] for element in line.split(',') if element in string_to_integer.keys())
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
        nltk_stopwords = stopwords.words('english')
        with open('additional_stopwords.txt', 'r') as file:
            extra_stopwords = [line.strip() for line in file.readlines()]

        extra_stopwords.extend(nltk_stopwords)
        extra_stopwords.extend(['links', 'external', 'see', 'may', 'refer', 'link', 'wa'])

        porter_stemmer = PorterStemmer()
        extra_stopwords_stemmed = [porter_stemmer.stem(word) for word in extra_stopwords]

        stopwords_set = set(extra_stopwords_stemmed)


        #stopwords_set = set(stopwords.words('english'))
        data = 'data/combined_stemmed.csv'
        # data = 'data/pruned.csv'
        block_size = "100MB"

        frequent_itemsets, string_mapping = apriori_disk(data_file=data,
                                                         exclude=stopwords_set,
                                                         min_support_percent=.17,
                                                         blocksize=block_size)

        if not frequent_itemsets or not string_mapping:
            exit()

        freq_itemsets_list = list(frequent_itemsets.keys())
        with open('data/frequent_itemsets.pkl', 'wb') as f:
            pickle.dump(frequent_itemsets, f)

        # if you want to recreate the list with the original strings
        for freq_itemset, support in frequent_itemsets.items():
            # Suppose you have a set of integers called int_set
            reconstructed_strings = set(key for key, value in string_mapping.items() if value in freq_itemset)
            print(reconstructed_strings, ":", str(support))

        #worried that this variable is actually too big, maybe I need to save it to .txt line by line instead
        itemset_features = check_itemsets(data, freq_itemsets_list, string_mapping, block_size)

        with open('data/itemset_features.pkl', 'wb') as f:
            pickle.dump(itemset_features, f)

        with open('data/itemset_list.pkl', 'wb') as f:
            pickle.dump(freq_itemsets_list, f)

        with open('data/string_mapping.pkl', 'wb') as f:
            pickle.dump(string_mapping, f)

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
            f.write(traceback.format_exc())
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
