import json
from dev.query_parser import QueryParser


def _positive_terms(node, negated=False):
    """Рекурсивно собирает позитивные (не отрицаемые) термы из AST."""
    from dev.ast import TermNode, NearNode, NotNode, AndNode, OrNode, AndWithPositivesAndNegativesNode
    if isinstance(node, TermNode):
        return [] if negated else [node.term]
    if isinstance(node, NotNode):
        return _positive_terms(node.operand, negated=True)
    if isinstance(node, NearNode):
        if negated:
            return []
        return [t.term for t in node.terms]
    if isinstance(node, (AndNode, OrNode)):
        terms = []
        for child in node.operands:
            terms += _positive_terms(child, negated)
        return terms
    if isinstance(node, AndWithPositivesAndNegativesNode):
        terms = []
        for child in node.pos_operands:
            terms += _positive_terms(child, negated)
        for child in node.neg_operands:
            terms += _positive_terms(child, negated=True)
        return terms
    return []
