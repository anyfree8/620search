import re
from typing import Dict, List, Set, Tuple, Any


class BooleanQueryParser:
    """Парсер булевых запросов"""

    def __init__(self):
        self.operators = ['AND', 'OR', 'NOT']
        self.field_pattern = r'(\w+):(\w+)'

    def tokenize_query(self, query: str) -> List[str]:
        """Токенизация запроса"""
        # Обработка кавычек для фразовых запросов
        query = query.strip()
        tokens = []

        # Простое разбиение по пробелам с учетом операторов
        parts = re.split(r'\s+', query)

        for part in parts:
            if part.upper() in self.operators:
                tokens.append(part.upper())
            elif ':' in part and re.match(self.field_pattern, part):
                tokens.append(part.lower())
            else:
                tokens.append(part.lower())

        return tokens

    def parse_query(self, query: str) -> Dict[str, Any]:
        """Парсинг булева запроса в структуру"""
        tokens = self.tokenize_query(query)

        # Простая структура запроса
        parsed_query = {
            'type': 'boolean',
            'must': [],      # AND условия
            'should': [],    # OR условия  
            'must_not': [],  # NOT условия
            'fields': {}     # поля документа
        }

        i = 0
        current_operator = 'AND'  # По умолчанию

        while i < len(tokens):
            token = tokens[i]

            if token in self.operators:
                current_operator = token
                i += 1
                continue

            # Проверка на поле документа
            if ':' in token:
                field_match = re.match(self.field_pattern, token)
                if field_match:
                    field_name, field_value = field_match.groups()
                    parsed_query['fields'][field_name] = field_value
                    i += 1
                    continue

            # Обычный терм
            if current_operator == 'AND':
                parsed_query['must'].append(token)
            elif current_operator == 'OR':
                parsed_query['should'].append(token)
            elif current_operator == 'NOT':
                parsed_query['must_not'].append(token)

            i += 1

        return parsed_query
