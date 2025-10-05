#!/usr/bin/env python3
"""
Скрипт для загрузки тестовых коллекций документов для поискового движка
"""

import os
import json
# import requests
# import time
from pathlib import Path
# from typing import List, Dict, Any

class DatasetLoader:
    """Класс для загрузки и подготовки коллекций документов"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.documents = []

    def load_sample_documents(self):
        """Загружает образцы документов разных типов"""
        print("Создание образцов документов...")

        sample_docs = [
            {
                'id': 'tech_1',
                'content': 'Machine Learning is a subset of artificial intelligence that enables computers to learn and make decisions without being explicitly programmed. It involves algorithms that can identify patterns in data and make predictions or classifications based on those patterns. Common applications include recommendation systems, image recognition, natural language processing, and autonomous vehicles. Popular libraries include scikit-learn, TensorFlow, and PyTorch.',
                'fields': {
                    'title': 'Introduction to Machine Learning',
                    'author': 'Tech Expert',
                    'category': 'technology',
                    'keywords': 'machine learning, AI, algorithms, data science'
                }
            },
            {
                'id': 'science_1', 
                'content': 'Quantum computing represents a revolutionary approach to computation that leverages quantum mechanical phenomena such as superposition and entanglement. Unlike classical computers that use bits (0 or 1), quantum computers use quantum bits or qubits that can exist in multiple states simultaneously. This allows quantum computers to potentially solve certain problems exponentially faster than classical computers, particularly in cryptography, optimization, and simulation of quantum systems.',
                'fields': {
                    'title': 'Quantum Computing Fundamentals',
                    'author': 'Physics Researcher',
                    'category': 'science',
                    'keywords': 'quantum computing, qubits, superposition, entanglement'
                }
            },
            {
                'id': 'business_1',
                'content': 'Digital transformation has become a critical strategic imperative for organizations across all industries. It involves integrating digital technology into all areas of business, fundamentally changing how companies operate and deliver value to customers. Key components include cloud computing, data analytics, artificial intelligence, Internet of Things (IoT), and mobile technologies. Successful digital transformation requires not just technology adoption but also cultural change and new business models.',
                'fields': {
                    'title': 'Digital Transformation Strategy',
                    'author': 'Business Analyst',
                    'category': 'business',
                    'keywords': 'digital transformation, cloud computing, business strategy'
                }
            },
            {
                'id': 'health_1',
                'content': 'Telemedicine has emerged as a vital component of modern healthcare delivery, especially following the COVID-19 pandemic. It enables remote clinical services through telecommunications technology, allowing patients to consult with healthcare providers without in-person visits. Benefits include increased access to care, reduced costs, and improved convenience for patients in rural or underserved areas. Technologies involved include video conferencing, remote monitoring devices, and electronic health records integration.',
                'fields': {
                    'title': 'Telemedicine in Modern Healthcare',
                    'author': 'Medical Professional',
                    'category': 'healthcare',
                    'keywords': 'telemedicine, healthcare, remote monitoring, telehealth'
                }
            },
            {
                'id': 'programming_1',
                'content': 'Python is a high-level, interpreted programming language known for its simplicity and readability. It supports multiple programming paradigms including procedural, object-oriented, and functional programming. Python has extensive libraries for web development (Django, Flask), data science (pandas, numpy), machine learning (scikit-learn, tensorflow), and automation. Its syntax emphasizes code readability and allows developers to express concepts in fewer lines of code.',
                'fields': {
                    'title': 'Python Programming Language',
                    'author': 'Software Developer',
                    'category': 'programming',
                    'keywords': 'Python, programming, web development, data science'
                }
            },
            {
                'id': 'web_1',
                'content': 'Web development involves creating websites and web applications using various technologies. Frontend development focuses on user interface and experience using HTML, CSS, and JavaScript. Backend development handles server-side logic, databases, and API integration using languages like Python, Java, or Node.js. Modern web development also includes responsive design, progressive web apps, and single-page applications using frameworks like React, Vue, or Angular.',
                'fields': {
                    'title': 'Modern Web Development',
                    'author': 'Web Developer', 
                    'category': 'web-development',
                    'keywords': 'web development, HTML, CSS, JavaScript, frameworks'
                }
            },
            {
                'id': 'data_1',
                'content': 'Data science is an interdisciplinary field that combines statistics, mathematics, computer science, and domain expertise to extract insights from structured and unstructured data. The data science process typically includes data collection, cleaning, exploration, modeling, and interpretation. Key tools include Python/R for programming, SQL for database queries, and visualization libraries like matplotlib and plotly. Machine learning algorithms are often used for predictive modeling.',
                'fields': {
                    'title': 'Introduction to Data Science',
                    'author': 'Data Scientist',
                    'category': 'data-science', 
                    'keywords': 'data science, statistics, machine learning, analytics'
                }
            },
            {
                'id': 'ai_1',
                'content': 'Artificial Intelligence encompasses various technologies that enable machines to perform tasks that typically require human intelligence. This includes machine learning, natural language processing, computer vision, and robotics. AI applications are widespread in industries like healthcare (diagnostic imaging), finance (fraud detection), transportation (autonomous vehicles), and entertainment (recommendation systems). Deep learning, a subset of machine learning, has revolutionized AI capabilities.',
                'fields': {
                    'title': 'Artificial Intelligence Overview',
                    'author': 'AI Researcher',
                    'category': 'artificial-intelligence',
                    'keywords': 'artificial intelligence, machine learning, deep learning, NLP'
                }
            }
        ]

        self.documents.extend(sample_docs)
        print(f"  Добавлено {len(sample_docs)} образцов документов")

    def save_documents(self, filename: str = "documents.json"):
        """Сохраняет загруженные документы в JSON файл"""
        output_path = self.data_dir / filename

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.documents, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Сохранено {len(self.documents)} документов в {output_path}")
        return output_path

    def get_statistics(self):
        """Возвращает статистику по загруженным документам"""
        if not self.documents:
            return "Нет загруженных документов"

        categories = {}
        total_words = 0

        for doc in self.documents:
            category = doc['fields'].get('category', 'unknown')
            categories[category] = categories.get(category, 0) + 1

            word_count = len(doc['content'].split())
            total_words += word_count

        stats = {
            'total_documents': len(self.documents),
            'total_words': total_words,
            'average_words_per_doc': total_words / len(self.documents),
            'categories': categories
        }

        return stats

def main():
    """Основная функция для создания коллекции документов"""
    loader = DatasetLoader()

    print("🔄 Создание коллекции документов для поискового движка...")
    print("=" * 60)

    # Загружаем образцы документов
    loader.load_sample_documents()

    # Сохраняем документы
    output_file = loader.save_documents()

    # Показываем статистику
    stats = loader.get_statistics()
    print("\n📊 Статистика коллекции:")
    print(f"  Всего документов: {stats['total_documents']}")
    print(f"  Всего слов: {stats['total_words']:,}")
    print(f"  Среднее слов на документ: {stats['average_words_per_doc']:.1f}")
    print("  Категории:")
    for category, count in stats['categories'].items():
        print(f"    {category}: {count} документов")

    print("\n✅ Коллекция готова к использованию!")
    print(f"📁 Файл: {output_file}")

    return output_file

if __name__ == "__main__":
    main()
