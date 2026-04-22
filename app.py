import json
from flask import Flask, render_template, request, jsonify, abort

from data.data_loader import load_documents
from dev.search_engine import SearchEngine
from dev.query_parser import QueryParser

from help.helper import _positive_terms


engine = SearchEngine()
load_documents(engine=engine, file='data/documents_100k')

app = Flask(__name__)

@app.route('/')
def index():
    """Главная страница с поисковой формой"""
    return render_template('index.html')

@app.route('/search', methods=['GET', 'POST'])
def search():
    """Обработка поискового запроса"""
    if request.method == 'POST':
        query = request.json.get('query', '').strip()

        if not query:
            return jsonify({'error': 'Пустой запрос'}), 400

        try:
            # Выполняем поиск. Основное ранжирование — BM25 + proximity;
            # дополнительно возвращаем TF-IDF для отображения в UI.
            results = engine.rescored_search(query)

            # Форматируем результаты
            formatted_results = []
            for doc_id, scores in results[:10]:  # Топ-10 результатов
                doc = engine.documents.get(int(doc_id))
                if doc is None:
                    continue

                # DataIndex.get() возвращает dict с полями: doc_id, text, title, url
                doc_text = doc.get('text', '')
                doc_title = doc.get('title', 'No title')
                doc_url = doc.get('url', '')

                tf_idf = round(scores['base_score'], 3)
                bm25_prox = round(scores['score'], 3)

                formatted_results.append({
                    'id': doc_id,
                    # score оставлен для обратной совместимости и равен
                    # первичному ранжирующему скору (BM25 + proximity).
                    'score': bm25_prox,
                    'base_score': tf_idf,
                    'title': doc_title,
                    'url': doc_url,
                    'content': doc_text,
                    'snippet': doc_text[:200] + '...' if len(doc_text) > 200 else doc_text
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
    try:
        total_documents = 0
        if getattr(engine, 'documents', None) is not None:
            try:
                total_documents = getattr(engine.documents, 'total_documents', len(engine.documents))
            except Exception:
                total_documents = 0

        total_terms = engine.rev_indexer.total_terms
        index_size = engine.rev_indexer.index_size

        return jsonify({
            'total_documents': total_documents,
            'total_terms': total_terms,
            'index_size': index_size,
        })
    except Exception as e:
        return jsonify({
            'total_documents': 0,
            'total_terms': 0,
            'index_size': 0,
            'error': f'failed_to_compute_stats: {str(e)}'
        }), 500

@app.route('/doc/<doc_id>')
def document_detail(doc_id: str):
    """Страница с деталями документа"""
    doc = engine.documents.get(int(doc_id))
    if doc is None:
        abort(404)
    
    # DataIndex.get() возвращает dict с полями: doc_id, text, title, url
    details = {
        'id': doc_id,
        'content': doc.get('text', ''),
        'fields': {
            'title': doc.get('title', ''),
            'url': doc.get('url', ''),
        }
    }

    # Извлекаем позитивные термы из параметра q
    highlight_terms = []
    q = request.args.get('q', '').strip()
    if q:
        try:
            ast = QueryParser().parse(q)
            seen = set()
            for term in _positive_terms(ast):
                t = term.lower()
                if t not in seen:
                    seen.add(t)
                    highlight_terms.append(t)
        except Exception:
            pass

    return render_template('document.html', doc=details,
                           highlight_terms=json.dumps(highlight_terms, ensure_ascii=False))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
