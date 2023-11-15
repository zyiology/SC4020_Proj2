import dask.distributed
from nltk.corpus import stopwords, words
import dask.bag as db
from collections import defaultdict


def apriori_disk(data_file, exclude, min_support_percent, blocksize):
    with open(data_file, 'r') as f:
        row_count = sum(1 for row in f)
        min_support = int(row_count * min_support_percent)

    freq_itemsets = freq_new_level = get_frequent_item_sets_first_pass(data_file, exclude, min_support, blocksize)
    k = 1

    while True:
        k += 1
        if k > 4: break

        candidates = generate_candidate_itemsets(freq_new_level)
        pruned_candidates = prune_candidates(candidates, freq_itemsets)

        freq_new_level = get_frequent_item_sets(data_file, pruned_candidates, min_support, blocksize)

        if not freq_new_level:
            break

        freq_itemsets.update(freq_new_level)
        print(f"Frequent Itemsets Level {k} completed")

    print(freq_itemsets)

    return


# uses alternate function to count itemsets in line, because makes first pass of dataset
# more efficient
def get_frequent_item_sets_first_pass(data_file, exclude, min_support, blocksize):
    text = db.read_text(data_file, blocksize=blocksize)

    results = text.map(count_itemsets_in_line_first_pass, exclude).fold(combine_counts).compute()

    filtered_item_sets = {frozenset(key): value for key, value in results.items() if value > min_support}

    return filtered_item_sets


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


if __name__ == "__main__":
    client = dask.distributed.Client(n_workers=6, threads_per_worker=2)  # Adjust based on your CPU
    exclude = set(stopwords.words('english'))

    #apriori_disk('data/combined.csv', 1)
    #apriori_disk('data/output_5.csv',0.3)
    apriori_disk('data/pruned.csv', exclude, 0.2, "0.2MB")
