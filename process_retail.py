import pandas as pd
import numpy as np
import pickle

df = pd.read_csv('data/online_retail_II.csv')

print(set(df['Price']))
exit()

# columns: 'Invoice', 'StockCode', 'Description', 'Quantity', 'InvoiceDate',
#        'Price', 'Customer ID', 'Country'

df['Country'] = df['Country'].astype('category')
# Map each unique string in 'cluster' to a number
codes, uniques = pd.factorize(df['Country'])
df['CountryNo'] = codes
# Optionally, if you want to see the mapping
cluster_mapping = dict(enumerate(uniques))
print(cluster_mapping)

# Drop rows where any relevant column is NaN
df.dropna(subset=['Invoice', 'Customer ID', 'Country', 'StockCode'], inplace=True)

# Group by TransactionNo and aggregate ProductNo and CustomerNo
grouped = df.groupby('Invoice').agg({'StockCode': lambda x: ','.join(x.astype(str)),
                                           'Customer ID': 'first',
                                           'CountryNo': 'first'})

# Write ProductNo to a text file
with open('data/retail_transactions.txt', 'w') as f:
    for products in grouped['StockCode']:
        f.write(products + '\n')

# Write CustomerNo to another text file
with open('data/retail_customers.pkl', 'wb') as f:
    customers = np.array(grouped['Customer ID'], dtype=np.int32)
    pickle.dump(customers, f)

with open('data/retail_country.pkl', 'wb') as f:
    country = np.array(grouped['CountryNo'], dtype=np.int32)
    pickle.dump(country, f)
