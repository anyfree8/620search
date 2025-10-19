from typing import Optional, List, Dict, Set
import re
from abc import ABC, abstractmethod


class BasePostingList(ABC):
    """Abstract base class for posting list"""

    def __init__(
            self,
            doc_ids: List[int],
            term: Optional[str] = None,
            capacity: Optional[int] = None
            ):
        self.doc_ids = doc_ids
        self.term = term
        self.capacity = capacity
        self.cursor = 0

    @property
    @abstractmethod
    def cost(self) -> int:
        pass

    @property
    def size(self) -> int:
        return len(self.doc_ids)

    def next(self) -> Optional[int]:
        """Returns the current doc_id and moves to the next"""
        if self.cursor >= self.size:
            return None
        doc_id = self.doc_ids[self.cursor]
        self.cursor += 1
        return doc_id
    
    def advance(self, target: int) -> Optional[int]:
        """Moves to the first doc_id >= target"""
        while self.cursor < self.size:
            if self.doc_ids[self.cursor] >= target:
                return self.doc_ids[self.cursor]
            self.cursor += 1
        return None

    def peak(self):
        """Returns the current doc_id without moving to the next"""
        if self.cursor < self.size:
            return self.doc_ids[self.cursor]
        return None

    def reset(self):
        self.cursor = 0


class PostingList(BasePostingList):
    """
    Sorted linked list of doc_ids 
    with required methods next() and advance()
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def cost(self) -> int:
        """Cost of the posting list"""
        return len(self.doc_ids)

    def __repr__(self):
        return f"({self.doc_ids})"

class AntiPostingList(BasePostingList):
    """
    Anti-posting list - represents the complement of a posting list
    within a given segment size without materialization
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def cost(self) -> int:
        """Cost of the posting list"""
        return self.size - len(self.doc_ids)

    def __repr__(self):
        return f"NOT({self.doc_ids})" 
