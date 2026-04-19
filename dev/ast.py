from typing import List, Optional
from itertools import chain


class ASTNode():
    pass

class TermNode(ASTNode):
    def __init__(self, term: str, field: str):
        self.term = term
        self.field = field
    
    def __repr__(self):
        return f"Term({self.field}:{self.term})"

class NearNode(ASTNode):
    def __init__(self, terms: List[TermNode], max_dist: int):
        self.terms = terms
        self.k = max_dist
    
    def __repr__(self):
        return f"Near({' '.join([str(term) for term in self.terms])}, k={self.k})"

class NotNode(ASTNode):
    def __init__(self, operand: ASTNode):
        self.operand = operand
    
    def __repr__(self):
        return f"NOT({self.operand})"

class AndNode(ASTNode):
    def __init__(self, operands: List[ASTNode]):
        self.operands = operands
    
    def __repr__(self):
        return f"AND({', '.join(str(op) for op in self.operands)})"
    
class AndWithPositivesAndNegativesNode(ASTNode):
    def __init__(self, pos_operands: List[ASTNode], neg_operands: List[ASTNode]):
        self.pos_operands = pos_operands
        self.neg_operands = neg_operands
    
    def __repr__(self):
        return f"AND({', '.join(str(op) for op in chain(self.pos_operands, self.neg_operands))})"

class OrNode(ASTNode):
    def __init__(self, operands: List[ASTNode]):
        self.operands = operands
    
    def __repr__(self):
        return f"OR({', '.join(str(op) for op in self.operands)})"