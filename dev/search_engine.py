from typing import Dict, List, Set, Tuple, Any, Callable

import heapq
import dev.index

from dev.posting_list import BasePostingList, PostingList, AntiPostingList
from dev.query_parser import *


class SearchEngine:
    """Search engine with boolean queries"""

    def __init__(self, indexer=None, parser=None):
        self.rev_indexer = dev.index.ReverseIndex('data/wikipedia_delta_index.pb')  # DocumentIndexer()
        self.pos_indexer = dev.index.CoordinateIndex('data/wikipedia_pos_index.pb')
        self.parser = parser or QueryParser()       # BooleanQueryParser()
        # self.size = indexer segment size ?

    def execute(self, node: ASTNode) -> Union[ASTNode, BasePostingList]:
        """Execute AST"""
        
        if isinstance(node, TermNode):
            doc_ids = self.rev_indexer.get(node.term)
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
            # return self.execute_near([self.execute(TermNode(term)) for term in node.terms], k=node.k)
            raise ValueError("Near is not implemented yet.")

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

    def near_in(self, doc_id: int, terms: List[str], k=None) -> bool:
        """
        NEAR: checks if all terms appear within a k-width window in doc_id.
        """
        k = k or len(terms) # TODO: ..

        pls = [
            #PostingList(coord_index[doc_id][term])
            PostingList(self.pos_indexer.get(doc_id, term)) # Not implemented
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

    def execute_near(self, pls: List[PostingList], k=None) -> PostingList:
        """Execute NEAR operation"""

        terms = [pl.term for pl in pls]
        if None in terms:
            raise ValueError('Non-trivial NEAR operation')

        pl = self.execute_and(pls)
        
        return PostingList([doc_id for doc_id in pl.doc_ids if self.near_in(doc_id, terms, k=k)])
