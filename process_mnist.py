import pandas as pd
import pickle
import numpy as np


def convert_row_to_transaction(row, threshold=128):
    active_pixels = []
    for idx, pixel in enumerate(row):
        if pixel > threshold:
            # Convert index to row, column format
            row_idx = idx // 28  # MNIST images are 28x28
            col_idx = idx % 28
            active_pixels.append(f"{row_idx}_{col_idx}")
    return active_pixels

# Load MNIST dataset from CSV
mnist_df = pd.read_csv('data/mnist_train.csv')

# File to save the transactions
output_file_path = 'data/mnist_transactions.txt'


labels = []
with open(output_file_path, 'w') as file:
    for index, row in mnist_df.iterrows():
        labels.append(row.iloc[0])
        row = row.iloc[1:]
        transaction = convert_row_to_transaction(row)
        file.write(','.join(transaction) + '\n')

print(f"Transactions saved to {output_file_path}")

with open('data/mnist_labels.pkl', 'wb') as f:
    labels = np.array(labels)
    pickle.dump(labels, f)

