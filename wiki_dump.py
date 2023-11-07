import bz2
import csv
import io
import re
import random
from collections import Counter
from collections.abc import Generator
import os
from lxml import etree
from tqdm import tqdm
import wikitextparser as wtp
from html import unescape as htt

import numpy as np
import pandas as pd

# Replace with the path to your index and dump files
index_file_path = 'data/enwiki-latest-pages-articles-multistream-index.txt.bz2'
offsets_file_path = 'data/offsets.txt'
dump_file_path = 'data/enwiki-latest-pages-articles-multistream.xml.bz2'
output_csv_path = 'data/articles_items.csv'


# Function to extract and process an article's text to find unique words
def extract_and_process_article(offset, length, dump_path):
    with bz2.open(dump_path, 'rb', encoding='utf-8') as dump_file:
        dump_file.seek(offset)
        data = dump_file.read(length)
        # Now, decompress and process the data
        text = bz2.decompress(data).decode('utf-8')
        # Remove XML tags and other unwanted characters
        text = re.sub(r'<[^>]+>', '', text)  # naive XML tag removal
        text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
        words = re.findall(r'\w+', text.lower())  # convert to lowercase and split
        unique_words = list(set(words))  # get unique words
        return unique_words


def main():
    # Read the index file to get offsets and lengths for articles
    # Note: This is a simplification, you may need to handle multistream format properly
    offsets = get_page_offsets(index_file_path, offsets_file_path)
    for bz2 in get_bz2_byte_str(dump_file_path, offsets):
        df = get_articles(bz2)
        print(df)
        break

    return


    # Randomly sample offsets and lengths
    num_samples = 1000
    selected_offsets_lengths = random.sample(offsets_lengths, num_samples)
    # Create a CSV writer to store the transactions
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        for offset, length in selected_offsets_lengths:
            unique_words = extract_and_process_article(offset, length, dump_file_path)
            csvwriter.writerow(unique_words)

    # Now, you have a CSV file with each article's unique words as rows


def test():
    # extract_and_process_article(600)
    xml_file = open(dump_file_path, "rb")
    xml_file.seek(600)
    unzipper = bz2.BZ2Decompressor()

    block = xml_file.read(677416-600)
    out = unzipper.decompress(block)
    print(out)
    return


def get_page_offsets(path_index: str, path_index_clean: str) -> list[int]:
    """Get page offsets from wikipedia file or cached version

    Wikipedia provide an index file containing the list of articles with their
    respective id and offset from the start of the file. Since we are
    interested only on the offsets, we read the original file, provided by
    `path_index`, extract the offsets and store in another file (defined by
    `path_index_clean`) to speed up the process

    Args:
        path_index (str): Path to the original index file provided by Wikipedia
            (bz2 compressed version)
        path_index_clean (str): Path to our version, containing only offsets

    Returns:
        List[int]: List of offsets
    """
    # Get the list of offsets
    # If our new offset file was not created, it gets the information
    # from the index file
    if not os.path.isfile(path_index_clean):
        # Read the byte offsets from the index file
        page_offset = []
        last_offset = None
        with open(path_index, 'rb') as f:
            b_data = bz2.decompress(f.read()).split(b'\n')
            # Drop the last line (empty)
            if b_data[-1] == b'':
                b_data = b_data[:-1]
            for line in tqdm(b_data):
                offset = line.decode().split(':', 1)[0]
                if last_offset != offset:
                    last_offset = offset
                    page_offset.append(int(offset))

        with open(path_index_clean, 'w') as f:
            f.write(','.join([str(i) for i in page_offset]))
    else:
        with open(path_index_clean, 'r') as f:
            page_offset = [int(idx) for idx in f.read().split(',')]

    return page_offset


def get_bz2_byte_str(path_articles: str,
                     offset_list: list[int]) -> Generator[bytes, None, None]:
    """Read the multistream bz2 file using the offset list

    The offset list defines where the bz2 (sub)file starts and ends

    Args:
        path_articles (str): Path to the bz2 file containing the Wikipedia
            articles.
        offset_list (List[int]): List of byte offsets

    Yields:
        bytes: String of bytes corresponding to a set of articles compressed
    """
    with open(path_articles, "rb") as f:
        last_offset = offset_list[0]
        # Drop the data before the offset
        f.read(last_offset)
        for next_offset in offset_list[1:]:
            offset = next_offset - last_offset
            last_offset = next_offset
            yield f.read(offset)


def dewiki(text):
    text = wtp.parse(text).plain_text()  # wiki to plaintext
    text = htt(text)  # remove any HTML
    text = text.replace('\\n', ' ')  # replace newlines
    text = re.sub(r'\s+', ' ', text)  # replace excess whitespace
    return text


def process_pages(byte_string_compressed: bytes):
    """Process pages from a bz2 compressed Wikimedia dump.

    Args:
        byte_string_compressed (bytes): Byte string of the bz2 compressed XML data.

    """
    bz2d = bz2.BZ2Decompressor()
    byte_string = bz2d.decompress(byte_string_compressed)
    doc = etree.parse(io.BytesIO(b'<root>' + byte_string + b'</root>'))

    df = pd.DataFrame(index=['id', 'title', 'text'])

    for page in doc.xpath('/root/page'):
        title_elem = page.find('title')
        redirect = page.find('redirect')
        id_elem = page.find('id')
        text_elem = page.xpath('.//revision/text')

        if redirect is not None:
            continue  # Skip redirect pages
        if title_elem is not None:
            title_text = title_elem.text
            if "(disambiguation)" in title_text or ":" in title_text:
                continue  # Skip disambiguation or namespaced pages

        if text_elem and id_elem is not None:
            page_id = id_elem.text
            raw_text = text_elem[0].text if text_elem[0].text is not None else ''
            clean_text = dewiki(raw_text)
            yield clean_text#{'id': page_id, 'title': title_text, 'text': clean_text}


def get_articles(byte_string_compressed: bytes) -> pd.DataFrame:
    """Get a dataframe containing the set of articles from a bz2

    Args:
        byte_string_compressed (bytes): Byte string corresponding to the bz2
            stream

    Returns:
        pd.DataFrame: Dataframe with columns title and article
    """
    def _get_text(list_xml_el):
        """Return the list of content for a list of xml_elements"""
        return [el.text for el in list_xml_el]

    def _get_id(list_xml_el):
        """Return the list of id's for a list of xml_elements"""
        return [int(el.text) for el in list_xml_el]

    bz2d = bz2.BZ2Decompressor()
    byte_string = bz2d.decompress(byte_string_compressed)
    doc = etree.parse(io.BytesIO(b'<root> ' + byte_string + b' </root>'))

    # col_id = _get_id(doc.xpath('*/id'))
    # col_title = _get_text(doc.xpath('*/title'))
    # col_article = _get_text(doc.xpath('*/revision/text'))
    #
    # print_raw_xml = True
    # # If requested, print the raw XML of the first article
    # if print_raw_xml:
    #     #page_elements = doc.xpath('//p')
    #     if doc:
    #         print(etree.tostring(doc, pretty_print=True).decode('utf-8'))
    #     else:
    #         print("No page elements found.")
    #
    # df = pd.DataFrame([col_id, col_title, col_article],
    #                   index=['index', 'title', 'article']).T
    # df['index'] = df['index'].astype(np.int32)
    # return df


def get_unique_words(text, dictionary):
    text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
    article_words = re.findall(r'\w+', text.lower())  # convert to lowercase and split
    unique_words = set(article_words)  # get unique words

    filtered_words = unique_words.intersection(dictionary)

    return list(filtered_words)


if __name__ == "__main__":
    main()
    # test()
