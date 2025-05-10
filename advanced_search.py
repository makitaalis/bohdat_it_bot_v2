#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Улучшенный поиск по  ФИО и дате рождения
"""

import re
import logging
from typing import Dict, Any, Optional, List, Union
import traceback
from config import API_DEFAULT_LANG, API_DEFAULT_LIMIT
from api_client import api_client
from file_processing import extract_phones_recursive, SOURCE_PRIORITY

logger = logging.getLogger(__name__)


def search_by_name_dob(query: str, lang: Optional[str] = None,
                       limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Выполняет поиск по строке запроса, содержащей ФИО и дату рождения.
    Автоматически конвертирует дату в ISO формат и использует fallback-запросы.

    Args:
        query (str): Строка запроса в формате "Фамилия Имя ДД.ММ.ГГГГ"
        lang (str, optional): Язык результатов
        limit (int, optional): Лимит поиска

    Returns:
        Dict[str, Any]: Результаты поиска и извлеченные телефоны
    """
    # Парсим запрос
    parts = query.split()

    # Проверяем, есть ли дата рождения в запросе
    date_pattern = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
    date_of_birth = None
    name_parts = []

    for part in parts:
        if date_pattern.match(part):
            date_of_birth = part
        else:
            name_parts.append(part)

    # Определяем фамилию и имя из оставшихся частей
    surname = name_parts[0] if name_parts else ""
    name = name_parts[1] if len(name_parts) > 1 else ""

    # Если нашли дату, конвертируем в ISO формат
    if date_of_birth:
        d, m, y = date_of_birth.split(".")
        iso_date = f"{y}-{m}-{d}"
        logger.info(f"Дата преобразована из {date_of_birth} в ISO формат: {iso_date}")
        date_of_birth = iso_date

    # Строим основной запрос
    if date_of_birth:
        main_query = f"{surname} {name} {date_of_birth}".strip()
    else:
        main_query = f"{surname} {name}".strip()

    logger.info(f"Основной запрос: {main_query}")

    # Отправляем основной запрос
    response = api_client.make_request(
        query=main_query,
        lang=lang or API_DEFAULT_LANG,
        limit=limit or API_DEFAULT_LIMIT
    )

    # Проверяем на ошибки и используем fallback-запросы при необходимости
    if "Error code" in response or "List" not in response:
        logger.warning(f"Основной запрос вернул ошибку или пустой результат, пробуем fallback-запросы")

        if date_of_birth and surname:
            # Первый fallback: только фамилия + дата
            fallback_query1 = f"{surname} {date_of_birth}".strip()
            logger.info(f"Fallback запрос 1: {fallback_query1}")
            response_fb1 = api_client.make_request(
                query=fallback_query1,
                lang=lang or API_DEFAULT_LANG,
                limit=limit or API_DEFAULT_LIMIT
            )

            # Если первый fallback успешен, используем его
            if "List" in response_fb1 and "Error code" not in response_fb1:
                response = response_fb1
                logger.info("Успешно получены данные по fallback запросу 1")
            else:
                logger.warning(f"Fallback запрос 1 не вернул результатов, пробуем следующий")

        if "Error code" in response or "List" not in response:
            if date_of_birth and name:
                # Второй fallback: только имя + дата
                fallback_query2 = f"{name} {date_of_birth}".strip()
                logger.info(f"Fallback запрос 2: {fallback_query2}")
                response_fb2 = api_client.make_request(
                    query=fallback_query2,
                    lang=lang or API_DEFAULT_LANG,
                    limit=limit or API_DEFAULT_LIMIT
                )

                # Если второй fallback успешен, используем его
                if "List" in response_fb2 and "Error code" not in response_fb2:
                    response = response_fb2
                    logger.info("Успешно получены данные по fallback запросу 2")
                else:
                    logger.warning(f"Fallback запрос 2 не вернул результатов, пробуем следующий")

            if "Error code" in response or "List" not in response:
                if surname and name:
                    # Третий fallback: только ФИО без даты
                    fallback_query3 = f"{surname} {name}".strip()
                    logger.info(f"Fallback запрос 3: {fallback_query3}")
                    response_fb3 = api_client.make_request(
                        query=fallback_query3,
                        lang=lang or API_DEFAULT_LANG,
                        limit=limit or API_DEFAULT_LIMIT
                    )

                    # Если третий fallback успешен, используем его
                    if "List" in response_fb3 and "Error code" not in response_fb3:
                        response = response_fb3
                        logger.info("Успешно получены данные по fallback запросу 3")
                    else:
                        logger.warning(f"Fallback запрос 3 не вернул результатов")

    # Создаем результат с дополнительными данными
    result = {
        "api_response": response,
        "query": query,
        "phones": []
    }

    # Если есть результаты, извлекаем телефоны с приоритизацией источников
    if "List" in response and isinstance(response["List"], dict):
        # Получаем телефоны из каждой базы данных
        phones_by_source = {}

        for db_name, db_info in response["List"].items():
            if db_name == "No results found" or "Data" not in db_info:
                continue

            # Извлекаем телефоны из данной базы
            phones = extract_phones_recursive(db_info["Data"])
            if phones:
                phones_by_source[db_name] = phones
                logger.info(f"Найдено {len(phones)} телефонов в базе {db_name}")

        # Сортируем источники по приоритету
        sorted_sources = sorted(
            phones_by_source.keys(),
            key=lambda src: SOURCE_PRIORITY.get(src, 0),
            reverse=True
        )

        # Собираем телефоны, начиная с наиболее приоритетных источников
        all_phones = []
        for source in sorted_sources:
            logger.info(f"Обработка источника {source} с приоритетом {SOURCE_PRIORITY.get(source, 0)}")
            all_phones.extend(phones_by_source[source])

        # Удаляем дубликаты и сохраняем порядок (сначала из приоритетных источников)
        unique_phones = []
        for phone in all_phones:
            if phone not in unique_phones:
                unique_phones.append(phone)

        result["phones"] = unique_phones
        logger.info(f"Всего найдено уникальных телефонов: {len(unique_phones)}")

    return result