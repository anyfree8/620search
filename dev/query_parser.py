import re

from typing import Dict, List, Set, Tuple, Any, Union, Optional
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

class QueryParser:
    """Парсер булевых запросов с приоритетом: NOT > AND > OR"""
    operands = ["AND", "OR", "NOT"]
    
    def __init__(self):
        self.tokens = []
        self.position = 0
    
    def tokenize(self, query: str) -> List[str]:
        """Токенизация запроса"""
        # Разделяем по пробелам и скобкам, сохраняя операторы
        pattern = r'(\(|\)|AND|OR|NOT|\w+\:\:\w+|\w+|@\d+)'
        tokens = re.findall(pattern, query)

        def normalize(token):
            if token not in ('AND', 'OR' ,'NOT'):
                return token.lower()
            return token
        return [normalize(token) for token in tokens if token.strip()]
    
    def parse(self, query: str) -> ASTNode:
        self.tokens = self.tokenize(query)
        self.position = 0
        
        if not self.tokens:
            raise ValueError("Пустой запрос")
        
        result = self.parse_or()
        if self.position < len(self.tokens):
            raise ValueError(f"Неожиданный токен: {self.tokens[self.position]}")
        
        return result
    
    def current_token(self) -> Optional[str]:
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def next_token(self) -> Optional[str]:
        return self.tokens[self.position + 1] if self.position + 1 < len(self.tokens) else None
    
    def consume(self, expected: str = None) -> str:
        if self.position >= len(self.tokens):
            raise ValueError("Неожиданный конец запроса")
        
        token = self.tokens[self.position]
        if expected and token != expected:
            raise ValueError(f"Ожидался {expected}, получен {token}")
        
        self.position += 1
        return token
    
    def parse_or(self) -> ASTNode:
        left = self.parse_and()
        
        operands = [left]
        while self.current_token() == "OR":
            self.consume("OR")
            operands.append(self.parse_and())
        
        return OrNode(operands) if len(operands) > 1 else operands[0]
    
    def parse_and(self) -> ASTNode:
        left = self.parse_not()
        
        operands = [left]
        while (self.current_token() and 
               self.current_token() not in ["OR", ")"] and 
               (self.current_token() == "AND" or 
                self.current_token() in ["NOT", "("] or 
                self.current_token().isalpha())):
            
            if self.current_token() == "AND":
                self.consume("AND")
            
            operands.append(self.parse_not())
        if len(operands) == 1:
            return operands[0]
        pos_operands = []
        neg_operands = []
        for op in operands:
            if isinstance(op, NotNode):
                neg_operands.append(op)
            else:
                pos_operands.append(op)
        if len(pos_operands) == 0 or len(neg_operands) == 0:
            return AndNode(operands) 
        return AndWithPositivesAndNegativesNode(pos_operands, neg_operands)
    
    def parse_not(self) -> ASTNode:
        """Парсинг NOT (наивысший приоритет)"""
        if self.current_token() == "NOT":
            self.consume("NOT")
            return NotNode(self.parse_primary())
        
        return self.parse_primary()
    
    def parse_primary(self) -> ASTNode:
        """Парсинг базовых элементов (термины и скобки)"""
        token = self.current_token()
        
        if token == "(":
            self.consume("(")
            result = self.parse_or()
            self.consume(")")
            return result
        phrase = []

        field = 'text'
        if token and self.next_token() is not None and self.next_token() not in self.operands and self.next_token() != ')' and token.find('::') != -1:
            tok = self.consume(token)
            field = tok[:tok.find('::')]
            tok = tok[tok.find('::') + 2:]
            phrase.append(tok)
            token = self.current_token()

        while token and self.next_token() is not None and self.next_token() not in self.operands and self.next_token() != ')':
            phrase.append(self.consume(token))
            token = self.current_token()
        near_k = None
        is_dist_token = re.match(r'^@\d+$', token)
        if token and not is_dist_token and token.find('::') == -1:
            phrase.append(self.consume(token))
        elif is_dist_token:
            near_k = int(self.consume(token)[1:])
        elif token.find('::') != -1:
            tok = self.consume(token)
            field = tok[:tok.find('::')]
            tok = tok[tok.find('::') + 2:]
            phrase.append(tok)

        if len(phrase) > 1:
            terms = [TermNode(term, field) for term in phrase]
            return NearNode(terms, near_k)
        else:
            return TermNode(phrase[0], field)
