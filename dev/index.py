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
        self.load_index()

    def get(self, term):
        doc_ids = []
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
        self.load_index()

    def get(self, doc_id, term):
        pos_ids = []
        for posting_list in self.index.postings:
            if posting_list.term == term:
                for positional_posting_lists in posting_list.postings:
                    if positional_posting_lists.doc_id == doc_id:
                        pos_ids = delta_decode(
                            positional_posting_lists.first_position,
                            list(positional_posting_lists.position_deltas)
                        )
        return pos_ids
    
    def load_index(self):
        with open(self.file_path, 'rb') as f:
            self.index.ParseFromString(f.read())