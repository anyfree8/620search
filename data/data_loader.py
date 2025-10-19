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
