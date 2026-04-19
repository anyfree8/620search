from dataclasses import dataclass
from typing import Union, List, Tuple, Dict, Any

import math
import heapq

from dev.index import ReverseIndex, CoordinateIndex, DataIndex
from dev.posting_list import BasePostingList, PostingList, AntiPostingList
from dev.ast import *
from dev.query_parser import QueryParser


REVERSE_INDEX_CONFIG_PATH = {
    'text_file_path': 'data/wikipedia_100k_delta_index_text.pb',
    'title_file_path': 'data/wikipedia_100k_delta_index_title.pb'
}

POS_INDEX_CONFIG_PATH = {
    'text_file_path': 'data/wikipedia_100k_pos_index_text.pb',
    'title_file_path': 'data/wikipedia_100k_pos_index_title.pb'
}

DATA_INDEX_CONFIG_PATH = {
    'file_path': 'data/documents_100k',
}


class SearchEngine:
    """Search engine with boolean queries"""
    rev_indexer: ReverseIndex
    pos_indexer: CoordinateIndex
    data_indexer: DataIndex
    parser: QueryParser
    documents: List[Dict[str, Any]]

    def __init__(
        self,
        reverse_index_paths=REVERSE_INDEX_CONFIG_PATH,
        pos_index_paths=POS_INDEX_CONFIG_PATH,
        data_index_paths=DATA_INDEX_CONFIG_PATH,
        parser=None,
    ):
        self.rev_indexer = ReverseIndex(**reverse_index_paths)
        self.pos_indexer = CoordinateIndex(**pos_index_paths)
        self.data_indexer = DataIndex(**data_index_paths)
        self.parser = parser or QueryParser()
        self.documents = None

    def _total_documents(self) -> int:
        return self.data_indexer.total_documents

    def _score_document(self, doc_id: int, positive_terms: List[TermNode]) -> float:
        score = 0.0
        for term_node in positive_terms:
            field = term_node.field or 'text'
            tf = self._tf(doc_id, term_node.term, field=field)
            if tf <= 0:
                continue
            score += (1.0 + math.log(tf)) * self._idf(term_node.term, field=field)
        return score
    
    def _tf(self, doc_id: int, term: str, field: str = 'text') -> int:
        positions = self.pos_indexer.get(doc_id, term, field=field)
        if positions is None:
            return 0
        return len(positions)
    
    def _df(self, term: str, field: str = 'text') -> int:
        doc_ids = self.rev_indexer.get(term, field=field)
        if doc_ids is None:
            return 0
        return len(doc_ids)
    
    def _idf(self, term: str, field: str = 'text') -> float:
        df = self._df(term, field=field)
        N = self._total_documents()
        return math.log((N + 1) / (df + 1)) + 1.0

    def search(self, query: str) -> List[Tuple[str, float]]:
        """Search by boolean query with TF-IDF ranking"""
        ast = self.parser.parse(query)
        matched_doc_ids = self.execute(ast).doc_ids

        positive_terms = self.collect_positives(ast)

        results = [
            (str(doc_id), self._score_document(doc_id, positive_terms))
            for doc_id in matched_doc_ids
        ]
        return results# .sort(key=lambda x: (-x[1], x[0]))
    
    def collect_positives(self, node: ASTNode) -> List[TermNode]:
        """Collect positive terms from AST."""

        if isinstance(node, TermNode):
            return [node]

        if isinstance(node, NotNode):
            return []

        if isinstance(node, AndWithPositivesAndNegativesNode):
            children = node.pos_operands
        elif isinstance(node, (AndNode, OrNode)):
            children = node.operands
        elif isinstance(node, NearNode):
            children = node.terms
        else:
            raise ValueError("Unknown AST")

        return [
            term
            for child in children
            for term in self.collect_positives(child)
        ]

    def execute(self, node: ASTNode) -> Union[ASTNode, BasePostingList]:
        """Execute AST"""
        
        if isinstance(node, TermNode):
            doc_ids = self.rev_indexer.get(node.term, field=node.field)
            return PostingList(doc_ids=doc_ids, term=node.term)
    
        if isinstance(node, NotNode):
            if isinstance(node.operand, NotNode):
                return self.execute(node.operand.operand)
            raise ValueError("Not is not implemented.")
    
        if isinstance(node, OrNode):
            return self.execute_or([self.execute(op) for op in node.operands])
    
        if isinstance(node, AndNode):
            return self.execute_and([self.execute(op) for op in node.operands])
    
        if isinstance(node, AndWithPositivesAndNegativesNode):
            pos = self.execute_and([self.execute(op) for op in node.pos_operands])
            neg = self.execute_and([self.execute(op.operand) for op in node.neg_operands])
            return self.execute_and_not(pos, neg)
    
        if isinstance(node, NearNode):
            if self.pos_indexer is None:
                raise ValueError("Near is not supplied yet.")
            return self.execute_near([self.execute(term) for term in node.terms], k=node.k, field=node.terms[0].field)

        raise ValueError("Unknown AST")
    
    @staticmethod
    def execute_or(pls: List[PostingList]) -> PostingList:
        """Execute OR operation through merging with skipping duplicates"""

        for pl in pls:
            pl.reset()
        
        doc_ids_q = []
        for j in range(len(pls)):
            doc_id = pls[j].next()
            if doc_id is not None:
                doc_ids_q.append((doc_id, j))
        
        heapq.heapify(doc_ids_q)
        union_doc_ids = []
        prev_doc_id = None
        
        while doc_ids_q:
            
            doc_id, j = heapq.heappop(doc_ids_q)
            if prev_doc_id is None or (doc_id != prev_doc_id):
                union_doc_ids.append(doc_id)
                prev_doc_id = doc_id
                
            doc_id = pls[j].next()
            if doc_id:
                heapq.heappush(doc_ids_q, (doc_id, j))
        
        return PostingList(union_doc_ids)

    @staticmethod
    def execute_and(pls: List[PostingList]) -> PostingList:
        """Execute AND operation"""
        if not pls:
            return PostingList([])

        for pl in pls:
            pl.reset()

        intersect_doc_ids = []
        doc_ids_q = [None] * len(pls)
        
        for j in range(len(pls)):
            doc_id = pls[j].peak()
            if doc_id is not None:
                doc_ids_q[j] = doc_id
            else:
                PostingList(intersect_doc_ids)

        while None not in doc_ids_q:
            is_all_equal = all([doc_ids_q[0] == doc_id for doc_id in doc_ids_q])
            if is_all_equal:
                intersect_doc_ids.append(doc_ids_q[0])
                {pls[j].next() for j in range(len(pls))}
                doc_ids_q = [pls[j].peak() for j in range(len(pls))]
            else:
                max_doc_id = max(doc_ids_q)
                doc_ids_q = [
                        pls[j].advance(max_doc_id) if doc_ids_q[j] < max_doc_id else doc_ids_q[j]
                        for j in range(len(pls))
                    ]
        
        return PostingList(intersect_doc_ids)

    @staticmethod
    def execute_and_not(pl: PostingList, not_pl: AntiPostingList) -> PostingList:
        """Execute X AND (NOT Y) operation"""
        
        if not_pl.cost == 0 or (not not_pl.doc_ids):
            return pl
        
        subtract_doc_ids = []

        pl.reset()
        not_pl.reset()

        doc_id = pl.peak()
        anti_doc_id = not_pl.peak()

        while doc_id is not None:
            if anti_doc_id is None or (doc_id < anti_doc_id):
                subtract_doc_ids.append(doc_id)
                pl.next()
                doc_id = pl.peak()
            elif doc_id == anti_doc_id:
                pl.next()
                not_pl.next()
                doc_id = pl.peak()
                anti_doc_id = not_pl.peak()
            else:
                anti_doc_id = not_pl.advance(doc_id)

        return PostingList(subtract_doc_ids)

    def near_in(self, doc_id: int, terms: List[str], k=None, field='text') -> bool:
        """
        NEAR: checks if all terms appear within a k-width window in doc_id.
        """
        k = k or len(terms) # TODO: ?

        pls = [
            PostingList(self.pos_indexer.get(doc_id, term, field=field))
            for term in terms
        ]

        pos_ids_q = [pls[j].peak() for j in range(len(pls))]

        while None not in pos_ids_q:
            min_pos_id = min(pos_ids_q)
            max_pos_id = max(pos_ids_q)
            if max_pos_id - min_pos_id + 1 <= k:
                return True
            idx = pos_ids_q.index(min_pos_id)
            pls[idx].next()
            pos_ids_q[idx] = pls[idx].peak()
        
        return False

    def execute_near(self, pls: List[PostingList], k=None, field='text') -> PostingList:
        """Execute NEAR operation"""

        terms = [pl.term for pl in pls]
        if None in terms:
            raise ValueError('Non-trivial NEAR operation')

        pl = self.execute_and(pls)
        
        return PostingList([doc_id for doc_id in pl.doc_ids if self.near_in(doc_id, terms, k=k, field=field)])
