import bz2
import csv
import io
import re
import random
from collections.abc import Generator
import os
from lxml import etree
from tqdm import tqdm
import concurrent.futures
import itertools
import numpy as np
import pandas as pd
from nltk.corpus import words
import multiprocessing

# FUNCTIONS FOR READING WIKI DATA: TAKEN FROM https://luizvbo.github.io/post/loading-wikipedia-data-with-python/
import wiki_dump

# Replace with the path to your index and dump files
index_file_path = 'data/enwiki-latest-pages-articles-multistream-index.txt.bz2'
offsets_file_path = 'data/offsets.txt'
dump_file_path = 'data/enwiki-latest-pages-articles-multistream.xml.bz2'
#output_csv_path = 'data/articles_items.csv'

english_words = set(words.words())


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
    csv_writers = setup_csv_writers(max_workers, output_csv_path)

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
                unique_words = wiki_dump.get_unique_words(text=page, dictionary=english_words)
                csvwriter.writerow(unique_words)


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
    output_csv_path_template = 'data/output_{}.csv'  # Template for output files

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
    main()