import heapq
from posting_list import BasePostingList, PostingList, AntiPostingList
from typing import Dict, List, Set, Tuple, Any, Callable


class SearchEngine:
    """Search engine with boolean queries"""
    
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
