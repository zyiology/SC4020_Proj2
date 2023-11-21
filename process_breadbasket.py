import pandas as pd
import numpy as np
import pickle

df = pd.read_csv('bread basket.csv')
# print(df.head())
df['cluster'] = df['period_day'] + df['weekday_weekend']
df['cluster'] = df['cluster'].astype('category')

# Drop rows where either ProductNo or CustomerNo is NaN
df.dropna(subset=['Item', 'cluster', 'Transaction'], inplace=True)

# Map each unique string in 'cluster' to a number
codes, uniques = pd.factorize(df['cluster'])
df['clusterNo'] = codes

# Optionally, if you want to see the mapping
cluster_mapping = dict(enumerate(uniques))

# Group by TransactionNo and aggregate ProductNo and CustomerNo
grouped = df.groupby('Transaction').agg({'Item': lambda x: ','.join(set(x.astype(str))),
                                           'clusterNo': 'first'})

# Write ProductNo to a text file
with open('data/breadbasket_transactions.txt', 'w') as f:
    for products in grouped['Item']:
        f.write(products + '\n')


with open('data/breadbasket_time.pkl', 'wb') as f:
    time = np.array(grouped['clusterNo'], dtype=np.int32)
    #print(customers)
    pickle.dump(time, f)


print(cluster_mapping)