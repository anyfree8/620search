# Веб-интерфейс
from flask import Flask, render_template, request, jsonify, abort
import json
import os

from typing import Dict, List, Set, Tuple, Any

from scripts.index import DocumentIndexer
from scripts.query_parser import BooleanQueryParser
from scripts.search_engine import SearchEngine
from data.data_loader import load_documents_from_json

# Инициализируем поисковый движок и загружаем тестовые данные
engine = SearchEngine()
load_documents_from_json(engine=engine, json_file = "data/documents.json")

app = Flask(__name__)

# Импортируем наш поисковый движок
# (В реальном проекте это будет отдельный модуль)

@app.route('/')
def index():
    """Главная страница с поисковой формой"""
    return render_template('index.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    """Обработка поискового запроса"""
    if request.method == 'POST':
        query = request.json.get('query', '').strip()
        max_distance = request.json.get('max_distance', None)

        if not query:
            return jsonify({'error': 'Пустой запрос'}), 400

        try:
            # Выполняем поиск
            results = engine.search(query, max_distance)

            # Форматируем результаты
            formatted_results = []
            for doc_id, score in results[:10]:  # Топ-10 результатов
                doc_content = engine.indexer.documents[doc_id]
                doc_fields = engine.indexer.doc_fields.get(doc_id, {})

                formatted_results.append({
                    'id': doc_id,
                    'score': round(score, 3),
                    'title': doc_fields.get('title', 'Без названия'),
                    'author': doc_fields.get('author', 'Неизвестный автор'),
                    'category': doc_fields.get('category', 'Без категории'),
                    'content': doc_content,
                    'snippet': doc_content[:200] + '...' if len(doc_content) > 200 else doc_content
                })

            return jsonify({
                'query': query,
                'total_results': len(results),
                'results': formatted_results
            })

        except Exception as e:
            return jsonify({'error': f'Ошибка поиска: {str(e)}'}), 500

    # GET запрос - показываем форму поиска
    query = request.args.get('q', '')
    return render_template('search.html', query=query)

@app.route('/api/documents', methods=['POST'])
def add_document():
    """API для добавления новых документов"""
    try:
        data = request.json
        doc_id = data.get('id')
        content = data.get('content')
        fields = data.get('fields', {})

        if not doc_id or not content:
            return jsonify({'error': 'Необходимы id и content'}), 400

        engine.add_document(doc_id, content, fields)

        return jsonify({'message': f'Документ {doc_id} добавлен успешно'})

    except Exception as e:
        return jsonify({'error': f'Ошибка добавления документа: {str(e)}'}), 500

@app.route('/api/stats')
def stats():
    """Статистика индекса"""
    return jsonify({
        'total_documents': engine.indexer.total_docs,
        'total_terms': len(engine.indexer.inverted_index),
        'index_size': sum(len(docs) for docs in engine.indexer.inverted_index.values())
    })

@app.route('/doc/<doc_id>')
def document_detail(doc_id: str):
    """Страница с деталями документа"""
    # Проверяем наличие документа в индексе
    doc_content = engine.indexer.documents.get(doc_id)
    if doc_content is None:
        abort(404)

    doc_fields = engine.indexer.doc_fields.get(doc_id, {})

    # Собираем все поля для отображения
    details = {
        'id': doc_id,
        'content': doc_content,
        'fields': doc_fields
    }

    return render_template('document.html', doc=details)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
