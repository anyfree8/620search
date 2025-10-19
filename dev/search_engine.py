from typing import Dict, List, Set, Tuple, Any, Callable

import heapq
from dev.posting_list import BasePostingList, PostingList, AntiPostingList
from dev.query_parser import *


class SearchEngine:
    """Search engine with boolean queries"""

    def __init__(self, indexer=None, parser=None):
        self.indexer = indexer or DocumentIndexer() # DocumentIndexer()
        self.parser = parser or QueryParser()       # BooleanQueryParser()
        # self.size = indexer segment size ?

    def execute(self, node: ASTNode) -> ASTNode | BasePostingList:
        """Execute AST"""
        
        if isinstance(node, TermNode):
            doc_ids = self.indexer.get(node.term, [])
            return PostingList(doc_ids=doc_ids)
    
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
