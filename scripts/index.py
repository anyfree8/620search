import re
import json
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Any
import math


class DocumentIndexer:
    """Класс для создания обратного и координатного индексов"""

    def __init__(self):
        self.inverted_index = defaultdict(set)  # term -> set of doc_ids
        self.positional_index = defaultdict(lambda: defaultdict(list))  # term -> doc_id -> [positions]
        self.documents = {}  # doc_id -> document content
        self.doc_fields = defaultdict(dict)  # doc_id -> field_name -> content
        self.term_doc_frequency = defaultdict(Counter)  # term -> Counter({doc_id: frequency})
        self.doc_lengths = {}  # doc_id -> document length
        self.total_docs = 0

    def preprocess_text(self, text: str) -> List[str]:
        """Предобработка текста: токенизация, приведение к нижнему регистру"""
        # Простая токенизация по словам
        text = text.lower()
        tokens = re.findall(r'\b\w+\b', text)
        return tokens

    def add_document(self, doc_id: str, content: str, fields: Dict[str, str] = None):
        """Добавление документа в индекс"""
        self.documents[doc_id] = content
        self.total_docs += 1

        # Обработка полей документа
        if fields:
            self.doc_fields[doc_id] = fields
            # Индексируем поля отдельно
            for field_name, field_content in fields.items():
                field_tokens = self.preprocess_text(field_content)
                for pos, token in enumerate(field_tokens):
                    field_term = f"{field_name}:{token}"
                    self.inverted_index[field_term].add(doc_id)
                    self.positional_index[field_term][doc_id].append(pos)
                    self.term_doc_frequency[field_term][doc_id] += 1

        # Основное содержимое документа
        tokens = self.preprocess_text(content)
        self.doc_lengths[doc_id] = len(tokens)

        for pos, token in enumerate(tokens):
            # Обратный индекс
            self.inverted_index[token].add(doc_id)

            # Координатный индекс
            self.positional_index[token][doc_id].append(pos)

            # Частота термов в документах
            self.term_doc_frequency[token][doc_id] += 1

    def get_term_frequency(self, term: str, doc_id: str) -> int:
        """Получить частоту терма в документе"""
        return self.term_doc_frequency[term][doc_id]

    def get_document_frequency(self, term: str) -> int:
        """Получить количество документов, содержащих терм"""
        return len(self.inverted_index[term])

    def calculate_tf_idf(self, term: str, doc_id: str) -> float:
        """Вычислить TF-IDF для терма в документе"""
        tf = self.get_term_frequency(term, doc_id)
        if tf == 0:
            return 0.0

        df = self.get_document_frequency(term)
        if df == 0:
            return 0.0

        # TF-IDF = (1 + log(tf)) * log(N/df)
        tf_score = 1 + math.log(tf)
        idf_score = math.log(self.total_docs / df)

        return tf_score * idf_score
