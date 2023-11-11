import dask.bag as db
from dask.distributed import Client
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords


def process_line(line, stemmer, exclude):
    words = line.split(',')

    stemmed_words = [stemmer.stem(word) for word in words if word not in exclude]
    return ','.join(stemmed_words)


if __name__ == "__main__":
    porter_stemmer = PorterStemmer()
    stop_words = set(stopwords.words('english'))

    client = Client(n_workers=6, threads_per_worker=2)  # Adjust based on your CPU

    # input_file_path = 'data/combined.csv'  # Replace with your input file path
    # output_file_path = 'data/combined_stem_*.csv'  # Replace with your desired output file path

    # input_file_path = 'data/output_5.csv'  # Replace with your input file path
    # output_file_path = 'data/big_test_stem_*.csv'  # Replace with your desired output file path

    input_file_path = 'data/articles_items.csv_worker_5.csv'  # Replace with your input file path
    output_file_path = 'data/test_stem_*.csv'  # Replace with your desired output file path

    bag = db.read_text(input_file_path, blocksize="2MB")
    result = bag.map(process_line, porter_stemmer, stop_words)

    # Write the lemmatized results to the output file
    result.to_textfiles(output_file_path)

    # Compute the Dask Bag to trigger the parallel processing
    result.compute()

