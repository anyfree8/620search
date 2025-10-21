import json
import data.wikipedia_dataset_pb2


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


def load_documents(engine, file: str = 'data/documents'):
    """Load the documents from protobuf file"""
    try:
        with open(file, 'rb') as f:
            collection = data.wikipedia_dataset_pb2.DocumentCollection()
            collection.ParseFromString(f.read())
            engine.documents = [
                {
                    'id': doc.id,
                    'content': doc.text,
                    'fields': {
                        'title': doc.title,
                        'url': doc.url,
                    }
                } 
                for doc in collection.documents
            ]
            # engine.documents = {int(doc.id): doc for doc in collection.documents}
        print(f"✅ Loaded {len(engine.documents)} documents from {file}")
    except Exception as e:
        print(f"❌ Error loading documents: {e}")
        engine.documents = None