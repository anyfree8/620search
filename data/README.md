# Датасеты и индексы

Этот проект использует protobuf-схему для хранения коллекции документов и индексов. Ниже краткая инструкция по генерации Python-классов и сборке индексов.

## 1) Сгенерировать Python-классы из .proto

Требуется установленный protoc (Protocol Buffers compiler).

```bash
protoc --python_out=. data/wikipedia_dataset.proto
```

После выполнения появится (или обновится) файл data/wikipedia_dataset_pb2.py.

## 2) Собрать датасет и индексы через make_dataset.py

Скрипт сборки находится в dev/make_dataset.py. Для быстрой сборки набора на 100k документов выполните в корне репозитория:

```bash
python -c "import dev.make_dataset as md; md.make_dataset('100k')"
```

Либо эквивалентно в интерактивном режиме Python:

```python
import dev.make_dataset as md
md.make_dataset('100k')
```

## Что будет создано

- Коллекция документов:
  - data/documents_100k
- Обратные индексы (delta-encoded):
  - data/wikipedia_100k_delta_index_text.pb
  - data/wikipedia_100k_delta_index_title.pb
- Позиционные индексы:
  - data/wikipedia_100k_pos_index_text.pb
  - data/wikipedia_100k_pos_index_title.pb

Эти пути используются приложением по умолчанию (см. dev/search_engine.py).

## Запуск приложения

Убедитесь, что файлы выше существуют, затем запустите сервер:

```bash
pip install -r requirements.txt
python app.py
```

После запуска откройте http://localhost:8080
