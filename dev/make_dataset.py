from datasets import load_dataset, Dataset

import datasets
import string
import data.wikipedia_dataset_pb2

from collections import defaultdict
from typing import List, Dict
from nltk.tokenize import WordPunctTokenizer

def delta_encode(sorted_ids: List[int]) -> tuple[int, List[int]]:
    first = sorted_ids[0]
    deltas = [sorted_ids[i] - sorted_ids[i - 1] for i in range(1, len(sorted_ids))]
    return first, deltas

def save_documents_to_protobuf(dataset, output_file: str):
    collection = data.wikipedia_dataset_pb2.DocumentCollection()
    
    for i, item in enumerate(dataset):
        doc = data.wikipedia_dataset_pb2.Document()
        doc.id = i
        doc.title = item.get('title', '')
        doc.text = item.get('text', '')
        doc.url = item.get('url', '')
        collection.id2document[i].CopyFrom(doc)
    
    with open(output_file, 'wb') as f:
        f.write(collection.SerializeToString())

def create_inverted_index_with_delta(
    documents,
    output_file,
    field='text'
):
    tokenizer = WordPunctTokenizer()
    term_to_docids = defaultdict(list)
    
    for doc_id, doc in enumerate(documents):
        text = doc.get(field, '').lower()
        words = tokenizer.tokenize(text)

        for word in words:
            if word not in string.punctuation and (word not in term_to_docids or term_to_docids[word][-1] != doc_id):
                term_to_docids[word].append(doc_id)

    index = data.wikipedia_dataset_pb2.InvertedIndex()
    
    for term, doc_ids in term_to_docids.items():
        postingList = data.wikipedia_dataset_pb2.PostingList();
        sorted_ids = sorted(doc_ids)
        first, deltas = delta_encode(sorted_ids)
        postingList.first_id = first
        postingList.deltas.extend(deltas)
        index.term2postingList[term].CopyFrom(postingList)
    
    with open(output_file, 'wb') as f:
        f.write(index.SerializeToString())

def create_positional_index(
    documents,
    output_file,
    field='text'
):
    index = data.wikipedia_dataset_pb2.PositionalIndex()
    tokenizer = WordPunctTokenizer()
    positional_index = defaultdict(lambda: defaultdict(list))
    
    for doc_id, doc in enumerate(documents):            
        text = doc.get(field, '').lower()

        for position, term in enumerate(tokenizer.tokenize(text)):
            if term not in string.punctuation:
                positional_index[doc_id][term].append(position)
    
    for doc_id, term_positions in positional_index.items():
        positionalPosting = data.wikipedia_dataset_pb2.PositionalPosting()
        for term in term_positions.keys():
            positions_list = data.wikipedia_dataset_pb2.PositionalPostingList()
            positions = sorted(term_positions[term])

            if positions:
                first, deltas = delta_encode(positions)
                positions_list.first_position = first
                positions_list.position_deltas.extend(deltas)
                positionalPosting.term2positionsList[term].CopyFrom(positions_list)
        index.docId2termPositionsLists[doc_id].CopyFrom(positionalPosting)
    with open(output_file, 'wb') as f:
        f.write(index.SerializeToString())

def save_dataset(size=150_000):
    stream = load_dataset(
        "wikimedia/wikipedia",
        "20231101.ru",
        split="train",
        streaming=True
    )

    subset_iter = stream.take(size)
    subset = Dataset.from_generator(lambda: subset_iter, features=stream.features)
    subset.save_to_disk(f"data/wikipedia_ru_{size}")

def make_dataset(size='100k'):
    dataset = datasets.load_from_disk(f"data/wikipedia_ru_{size}")
    save_documents_to_protobuf(dataset, f'data/documents_{size}')
    for field in ['text', 'title']:
        create_inverted_index_with_delta(dataset, f'data/wikipedia_{size}_delta_index_{field}.pb', field=field)
        create_positional_index(dataset, f'data/wikipedia_{size}_pos_index_{field}.pb', field=field)
    
