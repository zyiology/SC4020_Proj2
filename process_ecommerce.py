import pandas as pd
import numpy as np
import pickle

df = pd.read_csv('data/SalesTransaction.csv')

df['Country'] = df['Country'].astype('category')
# Map each unique string in 'cluster' to a number
codes, uniques = pd.factorize(df['Country'])
df['CountryNo'] = codes
# Optionally, if you want to see the mapping
cluster_mapping = dict(enumerate(uniques))
print(cluster_mapping)

# Drop rows where any relevant column is NaN
df.dropna(subset=['ProductNo', 'CustomerNo', 'Country', 'TransactionNo'], inplace=True)

# Group by TransactionNo and aggregate ProductNo and CustomerNo
grouped = df.groupby('TransactionNo').agg({'ProductNo': lambda x: ','.join(x.astype(str)),
                                           'CustomerNo': 'first',
                                           'CountryNo': 'first'})

# Write ProductNo to a text file
with open('data/ecommerce_transactions.txt', 'w') as f:
    for products in grouped['ProductNo']:
        f.write(products + '\n')

# Write CustomerNo to another text file
with open('data/ecommerce_customers.pkl', 'wb') as f:
    customers = np.array(grouped['CustomerNo'], dtype=np.int32)
    pickle.dump(customers, f)

# with open('data/ecommerce_country.pkl', 'wb') as f:
#     country = np.array(grouped['CountryNo'], dtype=np.int32)
#     #print(customers)
#     pickle.dump(country, f)
