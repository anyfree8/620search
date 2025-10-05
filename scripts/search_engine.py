from typing import Dict, List, Set, Tuple, Any

from scripts.index import DocumentIndexer
from scripts.query_parser import BooleanQueryParser


class SearchEngine:
    """Основной класс поискового движка"""

    def __init__(self):
        self.indexer = DocumentIndexer()
        self.parser = BooleanQueryParser()

    def add_document(self, doc_id: str, content: str, fields: Dict[str, str] = None):
        """Добавить документ в индекс"""
        self.indexer.add_document(doc_id, content, fields)

    def search(self, query: str, max_distance: int = None) -> List[Tuple[str, float]]:
        """Поиск документов по запросу"""
        parsed_query = self.parser.parse_query(query)

        # Получаем кандидатов на основе булевых условий
        candidates = self._boolean_search(parsed_query)

        # Если указано максимальное расстояние между термами
        if max_distance is not None and len(parsed_query['must']) > 1:
            candidates = self._proximity_search(parsed_query['must'], candidates, max_distance)

        # Ранжирование результатов
        ranked_results = self._rank_results(candidates, parsed_query)

        return ranked_results

    def _boolean_search(self, parsed_query: Dict[str, Any]) -> Set[str]:
        """Булев поиск"""
        result_docs = None

        # Обработка AND условий (must)
        for term in parsed_query['must']:
            term_docs = self.indexer.inverted_index.get(term, set())
            if result_docs is None:
                result_docs = term_docs.copy()
            else:
                result_docs &= term_docs

        # Обработка полей документа
        for field_name, field_value in parsed_query['fields'].items():
            field_term = f"{field_name}:{field_value}"
            field_docs = self.indexer.inverted_index.get(field_term, set())
            if result_docs is None:
                result_docs = field_docs.copy()
            else:
                result_docs &= field_docs

        # Обработка OR условий (should)
        if parsed_query['should']:
            should_docs = set()
            for term in parsed_query['should']:
                should_docs |= self.indexer.inverted_index.get(term, set())

            if result_docs is None:
                result_docs = should_docs
            else:
                result_docs |= should_docs

        # Обработка NOT условий (must_not)
        for term in parsed_query['must_not']:
            not_docs = self.indexer.inverted_index.get(term, set())
            if result_docs is not None:
                result_docs -= not_docs

        return result_docs or set()

    def _proximity_search(self, terms: List[str], candidates: Set[str], max_distance: int) -> Set[str]:
        """Поиск с ограничением расстояния между термами"""
        if len(terms) < 2:
            return candidates

        filtered_candidates = set()

        for doc_id in candidates:
            # Получаем позиции всех термов в документе
            term_positions = {}
            for term in terms:
                positions = self.indexer.positional_index[term].get(doc_id, [])
                if positions:
                    term_positions[term] = positions

            # Проверяем, есть ли все термы в документе
            if len(term_positions) == len(terms):
                # Проверяем расстояние между термами
                if self._check_proximity(term_positions, max_distance):
                    filtered_candidates.add(doc_id)

        return filtered_candidates

    def _check_proximity(self, term_positions: Dict[str, List[int]], max_distance: int) -> bool:
        """Проверка расстояния между термами"""
        # Получаем все позиции термов
        all_positions = []
        for term, positions in term_positions.items():
            for pos in positions:
                all_positions.append((pos, term))

        # Сортируем по позиции
        all_positions.sort()

        # Проверяем расстояние между соседними термами
        for i in range(len(all_positions) - 1):
            pos1, term1 = all_positions[i]
            pos2, term2 = all_positions[i + 1]

            if term1 != term2 and abs(pos2 - pos1) <= max_distance:
                return True

        return False

    def _rank_results(self, candidates: Set[str], parsed_query: Dict[str, Any]) -> List[Tuple[str, float]]:
        """Ранжирование результатов поиска"""
        ranked_results = []

        # Собираем все термы для ранжирования
        all_terms = parsed_query['must'] + parsed_query['should']
        field_terms = [f"{k}:{v}" for k, v in parsed_query['fields'].items()]
        all_terms.extend(field_terms)

        for doc_id in candidates:
            score = 0.0

            # Вычисляем общий TF-IDF скор
            for term in all_terms:
                tf_idf = self.indexer.calculate_tf_idf(term, doc_id)
                score += tf_idf

            ranked_results.append((doc_id, score))

        # Сортируем по убыванию релевантности
        ranked_results.sort(key=lambda x: x[1], reverse=True)

        return ranked_results
