import data.wikipedia_dataset_pb2

def delta_decode(first, deltas):
    result = [first]
    for delta in deltas:
        result.append(result[-1] + delta)
    return result

class ReverseIndex:
    def __init__(self, text_file_path = 'data/wikipedia_delta_index.pb', title_file_path = 'data/wikipedia_delta_index.pb'):
        self.text_index = data.wikipedia_dataset_pb2.InvertedIndex()
        self.title_index = data.wikipedia_dataset_pb2.InvertedIndex()
        self.text_file_path = text_file_path
        self.title_file_path = title_file_path
        self.load_indexes()

    # ---- Stats helpers / properties ----
    @staticmethod
    def _posting_list_length(pl):
        """Number of documents in posting list encoded with first_id + deltas."""
        return 1 + len(pl.deltas)

    @property
    def text_terms(self):
        return set(self.text_index.term2postingList.keys())

    @property
    def title_terms(self):
        return set(self.title_index.term2postingList.keys())

    @property
    def total_terms(self):
        return len(self.text_terms | self.title_terms)

    @property
    def index_size_text(self):
        return sum(self._posting_list_length(pl) for pl in self.text_index.term2postingList.values())

    @property
    def index_size_title(self):
        return sum(self._posting_list_length(pl) for pl in self.title_index.term2postingList.values())

    @property
    def index_size(self):
        return self.index_size_text + self.index_size_title

    def get(self, term, field=None):
        if field == 'text':
            return self.getByIndex(term, self.text_index)
        elif field == 'title':
            return self.getByIndex(term, self.title_index)
        else:
            return sorted(self.getByIndex(term, self.text_index) + self.getByIndex(term, self.title_index))

    def getByIndex(self, term, index):
        doc_ids = []
        posting_list = index.term2postingList.get(term, None)
        if posting_list is not None:
            doc_ids = delta_decode(
                posting_list.first_id,
                list(posting_list.deltas)
            )
        return doc_ids
    
    def load_indexes(self):
        with open(self.text_file_path, 'rb') as f:
            self.text_index.ParseFromString(f.read())
        with open(self.title_file_path, 'rb') as f:
            self.title_index.ParseFromString(f.read())

class CoordinateIndex:
    def __init__(self, text_file_path = 'data/wikipedia_delta_index.pb', title_file_path = 'data/wikipedia_delta_index.pb'):
        self.text_index = data.wikipedia_dataset_pb2.PositionalIndex()
        self.title_index = data.wikipedia_dataset_pb2.PositionalIndex()
        self.text_file_path = text_file_path
        self.title_file_path = title_file_path
        self.load_index()

    # Optional positional stats (can be used later if needed)
    @staticmethod
    def _positions_list_length(positions_list):
        # length of positions list = 1 + len(position_deltas)
        return 1 + len(positions_list.position_deltas)

    def get(self, doc_id, term, field='text'):
        if field == 'text':
            return self.getByIndex(doc_id, term, self.text_index)
        return self.getByIndex(doc_id, term, self.title_index)

    def getByIndex(self, doc_id, term, index):
        pos_ids = []
        term_positions_lists = index.docId2termPositionsLists.get(doc_id, None)
        if term_positions_lists is None:
            return pos_ids
        positions_list = term_positions_lists.term2positionsList.get(term, None)
        if positions_list is None:
            return pos_ids
        pos_ids = delta_decode(
            positions_list.first_position,
            list(positions_list.position_deltas)
        )
        return pos_ids
    
    def load_index(self):
        with open(self.text_file_path, 'rb') as f:
            self.text_index.ParseFromString(f.read())
        with open(self.title_file_path, 'rb') as f:
            self.title_index.ParseFromString(f.read())

class DataIndex:
    def __init__(self, file_path = 'data/documents'):
        self.index = data.wikipedia_dataset_pb2.DocumentCollection()
        self.file_path = file_path
        self.load_index()

    def get(self, doc_id):
        document = self.index.id2document.get(doc_id, None)
        if document is not None: 
            return {
                "doc_id": document.id,
                "text": document.text,
                "title": document.title,
                "url": document.url
            }
        return document

    def __len__(self):
        return len(self.index.id2document)

    @property
    def total_documents(self):
        return len(self)
            
    def load_index(self):
        with open(self.file_path, 'rb') as f:
            self.index.ParseFromString(f.read())