import json


def load_documents_from_json(engine, json_file: str = "data/documents.json"):
    """Загружает документы из JSON файла в поисковый движок"""
    from pathlib import Path

    json_path = Path(json_file)
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                documents = json.load(f)

            print(f"📖 Загружено {len(documents)} документов из {json_file}")

            for doc in documents:
                engine.add_document(doc['id'], doc['content'], doc['fields'])

            return True, len(documents)
        except Exception as e:
            print(f"❌ Ошибка загрузки из {json_file}: {e}")
    else:
        print(f"📁 Файл {json_file} не найден")

    return False, 0


# Добавляем тестовые документы
documents = [
    {
        'id': 'doc1',
        'content': 'Python programming language is powerful and easy to learn. It has extensive libraries for data science, web development, and machine learning.',
        'fields': {'title': 'Python Programming Guide', 'author': 'John Smith', 'category': 'programming'}
    },
    {
        'id': 'doc2', 
        'content': 'Machine learning with Python libraries like scikit-learn, TensorFlow and PyTorch. Building neural networks and training models.',
        'fields': {'title': 'Machine Learning with Python', 'author': 'Jane Doe', 'category': 'data-science'}
    },
    {
        'id': 'doc3',
        'content': 'Web development using Flask framework in Python. Creating REST APIs and building web applications with templates and databases.',
        'fields': {'title': 'Flask Web Development', 'author': 'Bob Johnson', 'category': 'web-development'}
    },
    {
        'id': 'doc4',
        'content': 'Data analysis and visualization with pandas and matplotlib. Processing CSV files, creating charts and statistical analysis.',
        'fields': {'title': 'Data Analysis Tutorial', 'author': 'Alice Brown', 'category': 'data-science'}
    },
    {
        'id': 'doc5',
        'content': 'Natural language processing techniques using NLTK and spaCy libraries. Text preprocessing, tokenization, and sentiment analysis.',
        'fields': {'title': 'NLP with Python', 'author': 'Charlie Wilson', 'category': 'data-science'}
    },
    {
        'id': 'doc6',
        'content': 'Building desktop applications with Python using Tkinter and PyQt. Creating graphical user interfaces and event handling.',
        'fields': {'title': 'Python GUI Development', 'author': 'Diana Prince', 'category': 'desktop-development'}
    },
    {
        'id': 'doc7',
        'content': 'Database integration with Python using SQLAlchemy and Django ORM. Working with PostgreSQL, MySQL and SQLite databases.',
        'fields': {'title': 'Python Database Programming', 'author': 'Eve Adams', 'category': 'database'}
    },
    {
        'id': 'doc8',
        'content': 'Automation and scripting with Python. File processing, system administration tasks, and task scheduling with cron jobs.',
        'fields': {'title': 'Python Automation Scripts', 'author': 'Frank Miller', 'category': 'automation'}
    }
]

# Индексируем документы
# for doc in documents:
#     engine.add_document(doc['id'], doc['content'], doc['fields'])