import numpy as np

# c = range(0,1000)
# a = np.linspace(start=0,stop=len(c),num=101,dtype=int)
# b = 0
#
# print(a)
#
# for i in c:
#     b+=1
#     if b in a:
#         print(str(b/len(c)*100) + "%")


# def chunked_offsets(offsets, chunk_size):
#     """Yield successive n-sized chunks from offsets with one overlap between each chunk."""
#     offsets = list(offsets)  # Ensure offsets is a list for easy indexing
#     for i in range(0, len(offsets), chunk_size):
#         if i != 0:
#             # Starting from the second chunk, include the last element of the previous chunk
#             yield offsets[i - 1:i + chunk_size]
#         else:
#             # For the first chunk, no overlap required
#             yield offsets[i:i + chunk_size]
#
#
# print(list(chunked_offsets([0,1,2,3,4,5,6,7,8,9,10], 3)))

# import pandas as pd
# import os
#
# directory = 'data/'
# file_list = []
#
# for filename in os.listdir(directory):
#     if filename.endswith(".csv") and filename.startswith("output_"):
#         file_list.append(os.path.join(directory, filename))
#
# combined_data = pd.DataFrame()
#
# for file in file_list:
#     df = pd.read_csv(file)
#     combined_data = combined_data.append(df, ignore_index=True)
#
# combined_data.to_csv('combined_data.csv', index=False)

combined_data = 'data/combined_stemmed.csv'
pruned = 'data/pruned_stemmed.csv'

with open(combined_data, 'r') as f:
    with open(pruned, 'w') as f2:
        for i,row in enumerate(f):
            f2.write(row)
            if i>25000:
                break
# #
# with open(combined_data, 'r') as f:
#     row_count = sum(1 for row in f if '|' in row)
#     print("no rows: ", row_count)
# import datetime
# with open('log.txt', 'w') as f:
#     now = datetime.datetime.now()
#     f.write(now.strftime("%Y-%m-%d %H:%M:%S"))

# import pickle
# with open('data/itemset_features.pkl', 'rb') as f:
#     my_itemset_features = pickle.load(f)
#     # for feat in my_itemset_features:
#     #     print(feat)
#     print("no of dimensions: ", my_itemset_features[0].size)
#     print(my_itemset_features[0])
#
# with open('data/frequent_itemsets.pkl', 'rb') as f:
#     itemsets = pickle.load(f)
#     print(itemsets)
#
# with open('data/string_mapping.pkl', 'rb') as f:
#     string_mapping = pickle.load(f)
#     print(string_mapping)
#
# import dask.bag as db
#
# data_file = 'data/combined_stemmed.csv'
# blocksize = "100MB"
# text = db.read_text(data_file, blocksize=blocksize)
# print(text.npartitions)

# itemsets_list = ['test']
# total_pairs = [(itemsets_list[i], itemsets_list[j]) for i in range(len(itemsets_list)) for j in
#                    range(i + 1, len(itemsets_list))]
#
# print(len(total_pairs))