import data.wikipedia_dataset_pb2

def delta_decode(first, deltas):
    if not deltas:
        return [first] if first else []
    
    result = [first]
    for delta in deltas:
        result.append(result[-1] + delta)
    return result

class ReverseIndex:
    def __init__(self, file_path = ''):
        self.index = data.wikipedia_dataset_pb2.InvertedIndex()
        self.file_path = file_path

    def get(self, term):
        for posting_list in self.index.postings:
            if posting_list.term == term:
                doc_ids = delta_decode(
                    posting_list.doc_ids.first_id,
                    list(posting_list.doc_ids.deltas)
                )
        return doc_ids
    
    def load_index(self):
        with open(self.file_path, 'rb') as f:
            self.index.ParseFromString(f.read())

class CoordinateIndex:
    def __init__(self, file_path = ''):
        self.index = data.wikipedia_dataset_pb2.PositionalIndex()
        self.file_path = file_path

    def get(self, term):
        for posting_list in self.index.postings:
            if posting_list.term == term:
                doc_ids = delta_decode(
                    posting_list.doc_ids.first_id,
                    list(posting_list.doc_ids.deltas)
                )
        return doc_ids
    
    def load_index(self):
        with open(self.file_path, 'rb') as f:
            self.index.ParseFromString(f.read())