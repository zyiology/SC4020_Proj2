import csv
import concurrent.futures
import itertools
import numpy as np
from nltk.corpus import words, stopwords
import multiprocessing
from dask.distributed import Client, as_completed
import dask
import dask.bag as db
from memory_profiler import profile
from nltk.stem import PorterStemmer

# FUNCTIONS FOR READING WIKI DATA: TAKEN FROM https://luizvbo.github.io/post/loading-wikipedia-data-with-python/
import wiki_dump

# Replace with the path to your index and dump files
index_file_path = 'data/enwiki-latest-pages-articles-multistream-index.txt.bz2'
offsets_file_path = 'data/offsets.txt'
dump_file_path = 'data/enwiki-latest-pages-articles-multistream.xml.bz2'
#output_csv_path = 'data/articles_items.csv'

english_words = set(words.words())
english_words_without_stops = english_words - set(stopwords.words('english'))

def process_stream_and_write_to_csv(stream, csvwriter):
    # Process each page in the stream
    for page in wiki_dump.process_pages(stream):
        unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words)
        csvwriter.writerow(unique_words)


def setup_csv_writers(num_workers, base_csv_path):
    writers = {}
    for i in range(num_workers):
        # Each worker will have its own CSV file
        csv_file_path = f"{base_csv_path}_worker_{i}.csv"
        csv_file = open(csv_file_path, 'w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        # Write headers or perform any other CSV setup here if necessary
        writers[i] = (csv_writer, csv_file)
    return writers


def main_fail():
    # Extract the page offsets
    offsets = wiki_dump.get_page_offsets(index_file_path, offsets_file_path)

    # Maximum number of workers, set this to the number of CPU cores you have
    max_workers = 6

    task_checkpoints = np.linspace(start=0, stop=len(offsets), num=1001, dtype=int)

    # Setup CSV writers for each worker
    csv_writers = setup_csv_writers(max_workers, None)

    # Use ProcessPoolExecutor for CPU-bound tasks
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Distribute the CSV writers evenly across the submitted tasks
        workers_cycle = itertools.cycle(range(max_workers))
        future_to_stream = {}

        for stream in wiki_dump.get_bz2_byte_str(dump_file_path, offsets):
            worker_index = next(workers_cycle)
            csvwriter, _ = csv_writers[worker_index]
            # Submit the task with the associated CSV writer
            future = executor.submit(process_stream_and_write_to_csv, stream, csvwriter)
            future_to_stream[future] = "temp"

        completed_tasks = 0

        # Process as tasks complete
        for future in concurrent.futures.as_completed(future_to_stream):
            stream = future_to_stream[future]
            completed_tasks += 1
            print(completed_tasks)
            try:
                future.result()
            except Exception as exc:
                print(f'{stream} generated an exception: {exc}')

            if completed_tasks in task_checkpoints:
                print(f"{(completed_tasks / len(offsets)) * 100}% complete")

    # Make sure to close all the CSV files
    for _, (csvwriter, csv_file) in csv_writers.items():
        csv_file.close()


def worker_process(offset_chunk, output_csv):
    # Each process will open its own output CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        for stream in wiki_dump.get_bz2_byte_str(dump_file_path, offset_chunk):
            for page in wiki_dump.process_pages(stream):
                unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words_without_stops)
                csvwriter.writerow(unique_words)


def process_pg(page):
    unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words_without_stops)
    return unique_words


#@profile
def worker_process_dask(chunk, output_csv):
    print(f"executing on {output_csv}...")
    # output_csv = output_csv_template.format(index)
    # Each process will open its own output CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        #print('csv file opened')
        for stream in wiki_dump.get_bz2_byte_str(dump_file_path, chunk):
            #print("looking at stream")
            for page in wiki_dump.process_pages(stream):
                #print('looking at page')
                unique_words = wiki_dump.get_unique_stemmed_words(text=page, dictionary=english_words)
                csvwriter.writerow(unique_words)
                #print('row written')
                del page
            del stream
            #print("stream deleted")

    # for stream in wiki_dump.get_bz2_byte_str(dump_file_path, chunk):
    #     for page in wiki_dump.process_pages(stream):
    #         unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words_without_stops)
    #         #test = page[0:10]
    #         del page
    #     del stream

    return None


def do_nothing(rubbish1, rubbish2):
    return None


def main_dask():
    dask.config.set({
        'distributed.scheduler.active-memory-manager.measure': 'managed',
        'distributed.worker.memory.rebalance.measure': 'managed',
        'distributed.worker.memory.spill': False,
        'distributed.worker.memory.pause': False,
        'distributed.worker.memory.terminate': False
    })

    client = Client(n_workers=6, threads_per_worker=1)
    number_of_workers = 6

    output_csv_path_template = 'data/batch/output_v2_{}.csv'
    offsets = list(wiki_dump.get_page_offsets(index_file_path, offsets_file_path))
    offset_chunks = list(chunked_offsets(offsets, len(offsets)//6 + 1))
    filenames = [output_csv_path_template.format(i) for i in range(len(offset_chunks))]

    def submit_task(i):
        if i < len(offset_chunks):
            return client.submit(worker_process_dask, offset_chunks[i], filenames[i])
        return None

    # List to store futures
    futures = []

    # Submit initial batch of tasks
    for i in range(6):  # Assuming 6 workers
        future = submit_task(i)
        if future is not None:
            futures.append(future)

    # Process tasks as they complete
    for future in as_completed(futures):
        i += 1  # Increment index
        new_future = submit_task(i)
        if new_future is not None:
            futures.append(new_future)

    # results = client.map(worker_process_dask, offset_chunks, filenames)
    # client.gather(results)

    #print(len(offsets))
    #print(sum([1 for _ in offset_chunks]))
    #futures = []
    # for i, chunk in enumerate(offset_chunks):
    #     future = client.submit(worker_process_dask, chunk, output_csv_path_template.format(i))
    #     fire_and_forget(future)
    #    futures.append(dask.delayed(worker_process_dask)(chunk, output_csv_path_template.format(i)))

    # offset_chunks_index = [(chunk, output_csv_path_template.format(i)) for i, chunk in enumerate(offset_chunks)]
    # db.from_sequence(offset_chunks_index, npartitions=1280).map(worker_process_dask_bag).fold(do_nothing).compute()

    # Using Dask to process each chunk in parallel
    # futures = [
    #     dask.delayed(worker_process_dask)(chunk, output_csv_path_template.format(i)) for i, chunk in enumerate(offset_chunks)]

    #results = dask.compute(*futures)

    return


def worker_process_dask_bag(input):
    print("executing...")
    chunk, output_csv = input
    # output_csv = output_csv_template.format(index)
    # Each process will open its own output CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        for stream in wiki_dump.get_bz2_byte_str(dump_file_path, chunk):
            for page in wiki_dump.process_pages(stream):
                unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words_without_stops)
                csvwriter.writerow(unique_words)
                del page
            del stream

    # for stream in wiki_dump.get_bz2_byte_str(dump_file_path, chunk):
    #     for page in wiki_dump.process_pages(stream):
    #         unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words_without_stops)
    #         #test = page[0:10]
    #         del page
    #     del stream

    return None

    # for i, chunk in enumerate(offset_chunks):
    #     streams = wiki_dump.get_bz2_byte_str(dump_file_path, chunk)
    #     streams
        #for stream in streams:
            #pages_bag = db.from_sequence(wiki_dump.process_pages(stream))
            #pages_bag.map(process_pg)

    #let's try to make streams the bag
    # if i have 1000 streams, 10 partitions - 1 parition has 100 streams - means I create 10 .txt files and each file has 100 elements
    # need to make map return 1 element as a multiline string - so the line stack?
    # def get_pages(offset_chunk):
    #     streams = wiki_dump.get_bz2_byte_str(dump_file_path, offset_chunk)
    #     processed_pages = []
    #     for stream in streams:
    #         processed_pages.extend(wiki_dump.process_pages(stream))
    #     return processed_pages
    #
    # # def get_pages(stream):
    # #     return wiki_dump.process_pages(stream)
    # #
    # # def get_pages_from_chunk(chunk):
    # #     streams = wiki_dump.get_bz2_byte_str(dump_file_path, chunk)
    # #     for stream in streams:
    # #         pages = wiki_dump.process_pages(stream)
    # #         yield pages
    #
    # bag = db.from_sequence(offset_chunks).map(get_pages)
    # result = bag.map(process_pg)
    #
    # #result.to_textfiles(output_csv_path_template) this can't work cos to_textfiles creates a textfile for each offset_chunk, one line for each element
    # result.compute()

    # client = Client(n_workers=6, threads_per_worker=1)
    #
    # number_of_workers = 6
    # output_csv_path_template = 'data/output_v2_*.csv'
    # offsets = list(wiki_dump.get_page_offsets(index_file_path, offsets_file_path))
    # #offset_chunks = chunked_offsets(offsets, len(offsets)//(number_of_workers*4)+1)
    # #print(sum(1 for _ in offset_chunks))
    #
    # streams = wiki_dump.get_bz2_byte_str(dump_file_path, offsets)
    # pages = [wiki_dump.process_pages(stream) for stream in streams]
    #
    # bag = db.from_sequence(pages)
    # result = bag.map(process_pg)
    #
    # result.to_textfiles(output_csv_path_template)
    # result.compute()
    #
    # return


def process_chunk(dump_file_path, offset_start, offset_end, output_csv_path):
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        stream = wiki_dump.get_bz2_byte_str(dump_file_path, [offset_start, offset_end])
        for page in wiki_dump.process_pages(next(stream)):
            unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words_without_stops)
            csvwriter.writerow(unique_words)


def main3():
    number_of_workers = 6
    client = Client(n_workers=number_of_workers, threads_per_worker=1)  # Starts a local Dask client

    output_csv_path_template = 'output_{}.csv'  # Template for output files

    # Get all offsets
    offsets = list(wiki_dump.get_page_offsets(index_file_path, offsets_file_path))

    # Create chunks with overlapping offsets
    chunk_size = len(offsets) // (number_of_workers*6)
    chunks = [(offsets[i - 1] if i else offsets[i], offsets[min(i + chunk_size, len(offsets) - 1)]) for i in
               range(0, len(offsets), chunk_size)]

    #chunks = chunked_offsets(offsets, len(offsets)//(number_of_workers*6)+1)

    # Using Dask to process each chunk in parallel
    futures = [
        dask.delayed(process_chunk)(dump_file_path, start, end, output_csv_path_template.format(i), english_words_without_stops) for
        i, (start, end) in enumerate(chunks)]
    results = dask.compute(*futures)


def chunked_offsets(offsets, chunk_size):
    """Yield successive n-sized chunks from offsets with one overlap between each chunk."""
    offsets = list(offsets)  # Ensure offsets is a list for easy indexing
    for i in range(0, len(offsets), chunk_size):
        if i != 0:
            # Starting from the second chunk, include the last element of the previous chunk
            yield offsets[i - 1:i + chunk_size]
        else:
            # For the first chunk, no overlap required
            yield offsets[i:i + chunk_size]


def main():
    # Configuration
    number_of_workers = 6 # Or set to the number of desired processes
    output_csv_path_template = 'data/batch/output_{}.csv'  # Template for output files

    # Get all offsets (could still be a generator if not too large, otherwise convert to list)
    offsets = list(wiki_dump.get_page_offsets(index_file_path, offsets_file_path))

    # Create a pool of workers
    with multiprocessing.Pool(number_of_workers) as pool:
        # Create jobs for the pool workers
        jobs = []
        for i, offset_chunk in enumerate(chunked_offsets(offsets, len(offsets) // number_of_workers)):
            output_csv_path = output_csv_path_template.format(i)  # Each worker writes to a different file
            job = pool.apply_async(worker_process, (offset_chunk, output_csv_path))
            jobs.append(job)

        # Wait for all jobs to complete
        for job in jobs:
            job.get()


if __name__ == "__main__":
    main_dask()
