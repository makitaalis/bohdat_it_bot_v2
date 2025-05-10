#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тестирование извлечения телефонных номеров
"""

import argparse
import logging
import sys
import json
from datetime import datetime

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("phone_extractor_test")

# Импортируем необходимые модули из проекта
try:
    from api_client import api_client
    from file_processing import extract_phones_recursive, SOURCE_PRIORITY

    # Импортируем новую функцию, если вы её создали
    try:
        from advanced_search import search_by_name_dob
    except ImportError:
        logger.warning("Модуль advanced_search не найден, используем прямой API-запрос")
        search_by_name_dob = None
except ImportError as e:
    logger.error(f"Ошибка импорта модулей: {e}")
    sys.exit(1)


def test_phone_extraction(query):
    """
    Тестирование извлечения телефонов из API-ответа
    """
    logger.info(f"Тестирование извлечения телефонов для запроса: {query}")

    # Используем функцию поиска, если доступна
    if search_by_name_dob is not None:
        logger.info("Используем функцию search_by_name_dob")
        result = search_by_name_dob(query)
        response = result.get("api_response", {})
        phones = result.get("phones", [])
    else:
        # Иначе делаем прямой запрос к API
        logger.info("Используем прямой запрос к API")
        # Проверяем, содержит ли запрос дату в формате DD.MM.YYYY
        import re
        if re.search(r"\d{2}\.\d{2}\.\d{4}", query):
            # Преобразуем дату в ISO формат
            parts = query.split()
            new_parts = []
            for part in parts:
                if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", part):
                    d, m, y = part.split(".")
                    new_parts.append(f"{y}-{m}-{d}")
                else:
                    new_parts.append(part)
            query = " ".join(new_parts)
            logger.info(f"Преобразованный запрос: {query}")

        response = api_client.make_request(query)

        # Если получили ошибку, выводим её и выходим
        if "Error code" in response:
            logger.error(f"Ошибка API: {response.get('Error code')} - {response.get('Status')}")
            return

        # Извлекаем телефоны
        phones = extract_phones_recursive(response)

    # Выводим результаты
    logger.info(f"Найдено телефонов: {len(phones)}")
    for i, phone in enumerate(phones, 1):
        logger.info(f"  {i}. {phone}")

    # Проверяем, содержит ли результат нужный номер
    target_phone = "79995367092"  # Замените на искомый номер
    if target_phone in phones:
        logger.info(f"✅ Целевой номер {target_phone} НАЙДЕН в результатах!")
    else:
        logger.warning(f"❌ Целевой номер {target_phone} НЕ НАЙДЕН в результатах!")

    # Выводим информацию о базах данных в ответе
    if "List" in response:
        logger.info(f"Базы данных в ответе API:")
        for db_name in response["List"].keys():
            if db_name == "No results found":
                logger.info(f"  - {db_name}")
                continue

            db_info = response["List"][db_name]
            record_count = len(db_info.get("Data", [])) if "Data" in db_info else 0
            priority = SOURCE_PRIORITY.get(db_name, 0)

            logger.info(f"  - {db_name} (приоритет: {priority}, записей: {record_count})")

    # Сохраняем ответ API в файл для анализа
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"api_response_{timestamp}.json"

    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(response, f, ensure_ascii=False, indent=2)
        logger.info(f"Ответ API сохранен в файл: {filename}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении ответа API: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Тестирование извлечения телефонных номеров")
    parser.add_argument("query", help="Запрос для тестирования (например, 'Хохлова Дарья 05.10.2005')")

    args = parser.parse_args()
    test_phone_extraction(args.query)