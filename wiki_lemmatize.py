import dask.bag as db
from dask.distributed import Client
from nltk.stem import PorterStemmer


def process_line(line, stemmer):
    words = line.split(',')
    stemmed_words = [stemmer.stem(word) for word in words]
    return ','.join(stemmed_words)


if __name__ == "__main__":
    porter_stemmer = PorterStemmer()

    client = Client(n_workers=6, threads_per_worker=2)  # Adjust based on your CPU

    input_file_path = 'data/combined.csv'  # Replace with your input file path
    output_file_path = 'data/combined_lemma_*.csv'  # Replace with your desired output file path

    bag = db.read_text(input_file_path,blocksize="100MB")
    result = bag.map(process_line, porter_stemmer)

    # Write the lemmatized results to the output file
    result.to_textfiles(output_file_path)

    # Compute the Dask Bag to trigger the parallel processing
    result.compute()

