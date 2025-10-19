import re

from typing import Dict, List, Set, Tuple, Any, Union, Optional
from itertools import chain

class ASTNode():
    """Базовый класс для узлов AST"""

class TermNode(ASTNode):
    """Узел для термина"""
    def __init__(self, term: str):
        self.term = term
    
    def __repr__(self):
        return f"Term({self.term})"

class NearNode(ASTNode):
    def __init__(self, terms: List[str], max_dist: int):
        self.terms = terms
        self.max_dist = max_dist
    
    def __repr__(self):
        return f"Near({' '.join(self.terms)}, k={self.max_dist})"

class NotNode(ASTNode):
    """Узел для операции NOT"""
    def __init__(self, operand: ASTNode):
        self.operand = operand
    
    def __repr__(self):
        return f"NOT({self.operand})"

class AndNode(ASTNode):
    """Узел для операции AND"""
    def __init__(self, operands: List[ASTNode]):
        self.operands = operands
    
    def __repr__(self):
        return f"AND({', '.join(str(op) for op in self.operands)})"
    
class AndWithPositivesAndNegativesNode(ASTNode):
    """Узел для операции AND"""
    def __init__(self, pos_operands: List[ASTNode], neg_operands: List[ASTNode]):
        self.pos_operands = pos_operands
        self.neg_operands = neg_operands
    
    def __repr__(self):
        return f"AND({', '.join(str(op) for op in chain(self.pos_operands, self.neg_operands))})"

class OrNode(ASTNode):
    """Узел для операции OR"""
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
        pattern = r'(\(|\)|AND|OR|NOT|\w+|@\d+)'
        tokens = re.findall(pattern, query)
        return [token for token in tokens if token.strip()]
    
    def parse(self, query: str) -> ASTNode:
        """Основная функция парсинга"""
        self.tokens = self.tokenize(query)
        self.position = 0
        
        if not self.tokens:
            raise ValueError("Пустой запрос")
        
        result = self.parse_or()
        if self.position < len(self.tokens):
            raise ValueError(f"Неожиданный токен: {self.tokens[self.position]}")
        
        return result
    
    def current_token(self) -> Optional[str]:
        """Получить текущий токен"""
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def next_token(self) -> Optional[str]:
        """Получить текущий токен"""
        return self.tokens[self.position + 1] if self.position + 1 < len(self.tokens) else None
    
    def consume(self, expected: str = None) -> str:
        """Потребить токен"""
        if self.position >= len(self.tokens):
            raise ValueError("Неожиданный конец запроса")
        
        token = self.tokens[self.position]
        if expected and token != expected:
            raise ValueError(f"Ожидался {expected}, получен {token}")
        
        self.position += 1
        return token
    
    def parse_or(self) -> ASTNode:
        """Парсинг OR (наименьший приоритет)"""
        left = self.parse_and()
        
        operands = [left]
        while self.current_token() == "OR":
            self.consume("OR")
            operands.append(self.parse_and())
        
        return OrNode(operands) if len(operands) > 1 else operands[0]
    
    def parse_and(self) -> ASTNode:
        """Парсинг AND (средний приоритет)"""
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

        while token and self.next_token() is not None and self.next_token() not in self.operands and self.next_token() != ')':
            phrase.append(self.consume(token))
            token = self.current_token()
        max_near_dist = 1
        is_dist_token = re.match(r'^@\d+$', token)
        if token and not is_dist_token:
            phrase.append(self.consume(token))
        elif is_dist_token:
            max_near_dist = int(self.consume(token)[1:])
        if len(phrase) > 1:
            return NearNode(phrase, max_near_dist)
        else:
            return TermNode(phrase[0])
