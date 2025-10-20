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
        doc = collection.documents.add()
        doc.id = str(item.get('id', i))
        doc.title = item.get('title', '')
        doc.text = item.get('text', '')
        doc.url = item.get('url', '')
    
    with open(output_file, 'wb') as f:
        f.write(collection.SerializeToString())

def create_inverted_index_with_delta(documents, output_file: str):
    tokenizer = WordPunctTokenizer()
    term_to_docids = defaultdict(list)
    
    for doc_id, doc in enumerate(documents):
        text = doc.get('text', '').lower()
        words = tokenizer.tokenize(text)

        for word in words:
            if word not in string.punctuation and (word not in term_to_docids or term_to_docids[word][-1] != doc_id):
                term_to_docids[word].append(doc_id)

    index = data.wikipedia_dataset_pb2.InvertedIndex()
    
    for term, doc_ids in term_to_docids.items():
        posting = index.postings.add()
        posting.term = term
        
        sorted_ids = sorted(doc_ids)
        first, deltas = delta_encode(sorted_ids)
        
        posting.doc_ids.first_id = first
        posting.doc_ids.deltas.extend(deltas)

        posting.frequencies.extend([1] * len(sorted_ids))
    
    with open(output_file, 'wb') as f:
        f.write(index.SerializeToString())

def create_positional_index(
    documents,
    output_file: str
):
    index = data.wikipedia_dataset_pb2.PositionalIndex()
    tokenizer = WordPunctTokenizer()
    positional_index = defaultdict(lambda: defaultdict(list))
    
    for doc_id, doc in enumerate(documents):            
        text = doc.get('text', '')

        for position, word in enumerate(tokenizer.tokenize(text)):
            if word not in string.punctuation:
                positional_index[word][doc_id].append(position)
    
    total_docs = 0
    total_terms = 0
    
    for term, doc_positions in positional_index.items():
        posting_list = index.postings.add()
        posting_list.term = term
        total_terms += 1
        
        for doc_id in sorted(doc_positions.keys()):
            positions = sorted(doc_positions[doc_id])
            
            posting = posting_list.postings.add()
            posting.doc_id = doc_id
            posting.frequency = len(positions)
            if positions:
                posting.first_position = positions[0]
                position_deltas = [
                    positions[i] - positions[i-1] 
                    for i in range(1, len(positions))
                ]
                posting.position_deltas.extend(position_deltas)
            
            total_docs = max(total_docs, doc_id + 1)
    index.total_documents = total_docs
    index.total_terms = total_terms
    with open(output_file, 'wb') as f:
        f.write(index.SerializeToString())

def save_dataset():
    stream = load_dataset(
        "wikimedia/wikipedia",
        "20231101.ru",
        split="train",
        streaming=True
    )

    subset_iter = stream.take(150_000)
    subset = Dataset.from_generator(lambda: subset_iter, features=stream.features)
    subset.save_to_disk("data/wikipedia_ru_100k")

def make_dataset():
    dataset = datasets.load_from_disk("data/wikipedia_ru_100k")
    save_documents_to_protobuf(dataset, 'data/documents')
    create_inverted_index_with_delta(dataset, 'data/wikipedia_delta_index.pb')

def make_pos_idx():
    dataset = datasets.load_from_disk("data/wikipedia_ru_100k")
    create_positional_index(dataset, 'data/wikipedia_pos_index.pb')
    

