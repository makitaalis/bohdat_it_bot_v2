#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для поиска информации по ФИО + дата рождения
Реализует улучшенный алгоритм поиска с нормализацией кириллицы,
обработкой разных форматов дат и скорингом телефонов
"""
import asyncio
import re
import json
import logging
from typing import Dict, List, Tuple, Any, Optional, Union
from datetime import datetime

from file_processing import extract_phones_from_api_response, evaluate_phone_confidence, extract_emails_from_response
from logger import logger


def standardize_russian_name(name_input: str) -> Dict[str, str]:
    """
    Стандартизация русского имени

    Args:
        name_input (str): Входная строка с именем

    Returns:
        Dict[str, str]: Словарь с компонентами имени
    """
    # Удаление лишних пробелов и приведение к нижнему регистру
    name = ' '.join(name_input.split()).lower()

    # Замена часто путаемых символов
    name = name.replace('ё', 'е')

    # Разделение на компоненты
    parts = name.split(' ', 2)

    if len(parts) == 3:
        surname, first_name, patronymic = parts
    elif len(parts) == 2:
        surname, first_name = parts
        patronymic = ""
    else:
        surname = parts[0]
        first_name = ""
        patronymic = ""

    return {
        "surname": surname,
        "first_name": first_name,
        "patronymic": patronymic,
        "full_name": name
    }


def standardize_birth_date(birth_date_input: str) -> Optional[str]:
    """
    Стандартизация даты рождения

    Args:
        birth_date_input (str): Входная строка с датой

    Returns:
        Optional[str]: Стандартизированная дата в формате YYYY-MM-DD или None при ошибке
    """
    try:
        # Обработка различных форматов
        if '.' in birth_date_input:
            day, month, year = birth_date_input.split('.')
            if len(year) == 2:  # Обработка 2-значных годов
                year = '19' + year if int(year) > 30 else '20' + year
            formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        elif '-' in birth_date_input:
            # Проверяем, если это уже формат YYYY-MM-DD
            parts = birth_date_input.split('-')
            if len(parts) == 3:
                if len(parts[0]) == 4:  # Если год идет первым
                    year, month, day = parts
                    formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:  # Если день идет первым (DD-MM-YYYY)
                    day, month, year = parts
                    formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            else:
                return None
        else:
            return None

        # Валидация даты
        year, month, day = map(int, formatted_date.split('-'))
        if not (1900 <= year <= 2025 and 1 <= month <= 12 and 1 <= day <= 31):
            return None

        return formatted_date
    except Exception as e:
        logger.error(f"Ошибка при стандартизации даты: {e}")
        return None


async def execute_search(name_input: str, birth_date_input: str, api_client) -> Dict[str, Any]:
    """
    Выполнение каскадного поиска по ФИО + дата рождения

    Args:
        name_input (str): Строка с ФИО
        birth_date_input (str): Строка с датой рождения
        api_client: Экземпляр API клиента

    Returns:
        Dict[str, Any]: Результаты поиска
    """
    name_data = standardize_russian_name(name_input)
    birth_date = standardize_birth_date(birth_date_input)

    if not birth_date:
        return {"error": "Неверный формат даты рождения"}

    # Запись в лог для отладки
    logger.info(f"Стандартизированные данные для поиска: {name_data}, дата: {birth_date}")

    # Создание основного запроса
    primary_query = f"{name_data['surname']} {name_data['first_name']} {birth_date}"
    if name_data['patronymic']:
        primary_query = f"{name_data['surname']} {name_data['first_name']} {name_data['patronymic']} {birth_date}"

    # Создание запросов с частичным совпадением
    fallback_queries = []

    # Запрос только по фамилии и имени, без отчества
    if name_data["patronymic"]:
        fallback_queries.append(f"{name_data['surname']} {name_data['first_name']} {birth_date}")

    # Запрос только по фамилии и дате рождения
    fallback_queries.append(f"{name_data['surname']} {birth_date}")

    # Выполнение каскадного поиска
    results = {}

    # Получаем event loop
    loop = asyncio.get_event_loop()

    # Логируем основной запрос
    logger.info(f"Отправка основного запроса: {primary_query}")

    # Сначала пробуем основной запрос (используем run_in_executor для вызова синхронной функции)
    primary_response = await loop.run_in_executor(
        None,
        lambda: api_client.search_by_name_dob(primary_query)
    )
    results["primary_response"] = primary_response

    # Если основной запрос не дал результатов, пробуем запросы с частичным совпадением
    if "error" in primary_response or not has_useful_data(primary_response):
        logger.info("Основной запрос не дал результатов, пробуем запросы с частичным совпадением")

        for i, fallback in enumerate(fallback_queries):
            logger.info(f"Отправка запроса с частичным совпадением {i + 1}: {fallback}")
            # Используем run_in_executor для вызова синхронной функции
            fallback_response = await loop.run_in_executor(
                None,
                lambda query=fallback: api_client.search_by_name_dob(query)
            )
            results[f"fallback_response_{i + 1}"] = fallback_response

            if not "error" in fallback_response and has_useful_data(fallback_response):
                break

    # Применяем фильтр сильного совпадения ко всем результатам
    filtered_results = apply_strong_match_filter(results, name_data, birth_date)
    results["filtered_results"] = filtered_results

    # Выделяем стабильные идентификаторы
    identifiers = extract_stable_identifiers(filtered_results)
    results["identifiers"] = identifiers

    # Если есть идентификаторы, выполняем второй запрос
    if identifiers:
        second_stage_results = await search_by_identifier(identifiers, api_client)
        results["second_stage_results"] = second_stage_results

        # Объединяем результаты первого и второго этапа
        all_results = merge_search_results(filtered_results, second_stage_results)

        # Применяем скоринг телефонов
        scored_phones = score_phones(all_results, name_data, birth_date)
        results["scored_phones"] = scored_phones

        # Выбираем лучший телефон
        best_phone = select_best_phone(scored_phones)
        if best_phone:
            results["best_phone"] = best_phone

    return results

def has_useful_data(response: Dict[str, Any]) -> bool:
    """
    Проверяет, содержит ли ответ API полезные данные

    Args:
        response (Dict[str, Any]): Ответ API

    Returns:
        bool: True, если есть полезные данные, иначе False
    """
    if "List" not in response:
        return False

    # Проверяем, есть ли в результатах что-то кроме "No results found"
    if len(response["List"]) == 1 and "No results found" in response["List"]:
        return False

    # Проверяем наличие данных в базах
    for db_name, db_info in response["List"].items():
        if db_name != "No results found" and "Data" in db_info and db_info["Data"]:
            return True

    return False


def apply_strong_match_filter(results: Dict[str, Any], name_data: Dict[str, str], birth_date: str) -> List[
    Dict[str, Any]]:
    """
    Применяет фильтр сильного совпадения к результатам поиска

    Args:
        results (Dict[str, Any]): Результаты поиска
        name_data (Dict[str, str]): Данные имени
        birth_date (str): Дата рождения

    Returns:
        List[Dict[str, Any]]: Отфильтрованные результаты
    """
    filtered_results = []

    # Обрабатываем каждый ответ API
    for response_key, response in results.items():
        if not isinstance(response, dict) or "List" not in response:
            continue

        for db_name, db_info in response["List"].items():
            if db_name == "No results found" or "Data" not in db_info:
                continue

            for record in db_info["Data"]:
                score = 0

                # Оценка совпадения фамилии (максимум 35 баллов)
                surname_score = 0
                for field_name, field_value in record.items():
                    field_lower = field_name.lower()
                    if "фамилия" in field_lower or "surname" in field_lower or "lastname" in field_lower:
                        if field_value:
                            surname_similarity = calculate_similarity(str(field_value).lower(), name_data["surname"])
                            surname_score = surname_similarity * 35
                            break

                # Проверка ФИО в полном поле
                for field_name, field_value in record.items():
                    field_lower = field_name.lower()
                    if "фио" in field_lower or "fullname" in field_lower or "full_name" in field_lower:
                        if field_value and name_data["surname"] in str(field_value).lower():
                            if surname_score < 20:  # Если фамилия еще не найдена с высокой точностью
                                surname_score = max(surname_score, 20)

                score += surname_score

                # Оценка совпадения имени (максимум 25 баллов)
                first_name_score = 0
                for field_name, field_value in record.items():
                    field_lower = field_name.lower()
                    if "имя" in field_lower or "firstname" in field_lower or "first_name" in field_lower:
                        if field_value and name_data["first_name"]:
                            first_name_similarity = calculate_similarity(str(field_value).lower(),
                                                                         name_data["first_name"])
                            first_name_score = first_name_similarity * 25
                            break

                # Проверка имени в полном поле
                if first_name_score == 0 and name_data["first_name"]:
                    for field_name, field_value in record.items():
                        field_lower = field_name.lower()
                        if "фио" in field_lower or "fullname" in field_lower or "full_name" in field_lower:
                            if field_value and name_data["first_name"] in str(field_value).lower():
                                first_name_score = 15  # Частичное совпадение

                score += first_name_score

                # Оценка совпадения отчества (максимум 15 баллов)
                if name_data["patronymic"]:
                    patronymic_score = 0
                    for field_name, field_value in record.items():
                        field_lower = field_name.lower()
                        if "отчество" in field_lower or "patronymic" in field_lower or "middlename" in field_lower:
                            if field_value:
                                patronymic_similarity = calculate_similarity(str(field_value).lower(),
                                                                             name_data["patronymic"])
                                patronymic_score = patronymic_similarity * 15
                                break

                    # Проверка отчества в полном поле
                    if patronymic_score == 0:
                        for field_name, field_value in record.items():
                            field_lower = field_name.lower()
                            if "фио" in field_lower or "fullname" in field_lower or "full_name" in field_lower:
                                if field_value and name_data["patronymic"] in str(field_value).lower():
                                    patronymic_score = 7  # Частичное совпадение

                    score += patronymic_score

                # Оценка совпадения даты рождения (максимум 25 баллов)
                birth_date_score = 0
                for field_name, field_value in record.items():
                    field_lower = field_name.lower()
                    if "birth" in field_lower or "рождения" in field_lower or "dob" in field_lower or "date" in field_lower:
                        if field_value:
                            # Точное совпадение даты
                            if birth_date == str(field_value) or convert_date_format(str(field_value)) == birth_date:
                                birth_date_score = 25
                                break
                            # Частичное совпадение (только год и месяц)
                            elif birth_date[:7] == str(field_value)[:7] or birth_date[:7] == convert_date_format(
                                    str(field_value))[:7]:
                                birth_date_score = 15
                                break
                            # Частичное совпадение (только год)
                            elif birth_date[:4] == str(field_value)[:4] or birth_date[:4] == convert_date_format(
                                    str(field_value))[:4]:
                                birth_date_score = 10
                                break

                score += birth_date_score

                # Добавляем источник
                record["_source_db"] = db_name
                record["_response_key"] = response_key
                record["_match_score"] = score

                # Добавляем запись, если она прошла порог сильного совпадения (50%)
                if score >= 50:
                    filtered_results.append(record)

    # Сортировка по убыванию оценки совпадения
    filtered_results.sort(key=lambda x: x.get("_match_score", 0), reverse=True)

    return filtered_results


def convert_date_format(date_str: str) -> str:
    """
    Конвертирует формат даты для сравнения

    Args:
        date_str (str): Строка с датой

    Returns:
        str: Дата в формате YYYY-MM-DD
    """
    try:
        # Распознаем формат DD.MM.YYYY
        if '.' in date_str and len(date_str.split('.')) == 3:
            day, month, year = date_str.split('.')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Распознаем формат YYYY-MM-DD
        if '-' in date_str and len(date_str.split('-')) == 3:
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # Если год идет первым
                return date_str
            else:  # Если день идет первым (DD-MM-YYYY)
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Если формат не распознан, возвращаем исходную строку
        return date_str
    except:
        return date_str


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Расчет сходства между строками (0-1)

    Args:
        str1 (str): Первая строка
        str2 (str): Вторая строка

    Returns:
        float: Значение сходства от 0 до 1
    """
    # Приведение к нижнему регистру и удаление пробелов
    s1 = str1.lower().strip()
    s2 = str2.lower().strip()

    # Расчет расстояния Левенштейна
    distance = levenshtein_distance(s1, s2)
    max_len = max(len(s1), len(s2))

    # Расчет сходства (0-1)
    if max_len == 0:
        return 0

    return 1 - (distance / max_len)


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Реализация расчета расстояния Левенштейна

    Args:
        s1 (str): Первая строка
        s2 (str): Вторая строка

    Returns:
        int: Расстояние Левенштейна
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def extract_stable_identifiers(filtered_results: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Извлечение стабильных идентификаторов из результатов

    Args:
        filtered_results (List[Dict[str, Any]]): Отфильтрованные результаты

    Returns:
        Dict[str, str]: Словарь с идентификаторами
    """
    identifiers = {}

    for record in filtered_results:
        # Извлечение email
        for field_name, field_value in record.items():
            field_lower = field_name.lower()

            # Поиск email
            if ("email" in field_lower or "почта" in field_lower or "mail" in field_lower) and field_value:
                if isinstance(field_value, str) and '@' in field_value:
                    email = field_value.lower().strip()
                    if "email" not in identifiers or not identifiers["email"]:
                        identifiers["email"] = email

            # Поиск VK ID
            elif ("vk" in field_lower or "вконтакте" in field_lower) and field_value:
                vk_id = extract_vk_id(str(field_value))
                if vk_id and ("vk_id" not in identifiers or not identifiers["vk_id"]):
                    identifiers["vk_id"] = vk_id

            # Поиск логина
            elif ("login" in field_lower or "логин" in field_lower or "username" in field_lower) and field_value:
                login = str(field_value).lower().strip()
                if "login" not in identifiers or not identifiers["login"]:
                    identifiers["login"] = login

            # Поиск номера паспорта
            elif ("passport" in field_lower or "паспорт" in field_lower) and field_value:
                passport = normalize_passport(str(field_value))
                if passport and ("passport" not in identifiers or not identifiers["passport"]):
                    identifiers["passport"] = passport

    return identifiers


def extract_vk_id(vk_string: str) -> Optional[str]:
    """
    Извлечение VK ID из строки

    Args:
        vk_string (str): Строка, которая может содержать VK ID

    Returns:
        Optional[str]: Извлеченный VK ID или None
    """
    vk_string = str(vk_string).lower().strip()

    # Извлечение числового ID
    if vk_string.isdigit():
        return vk_string

    # Извлечение ID из URL
    if "vk.com/" in vk_string:
        parts = vk_string.split("vk.com/")
        if len(parts) > 1:
            id_part = parts[1].split("?")[0].split("/")[0].strip()
            if id_part.startswith("id") and id_part[2:].isdigit():
                return id_part[2:]  # Возвращаем числовой ID без префикса "id"
            elif id_part.isdigit():
                return id_part

    # Регулярное выражение для поиска "id" + числа
    id_match = re.search(r'id(\d+)', vk_string)
    if id_match:
        return id_match.group(1)

    return None


def normalize_passport(passport_string: str) -> Optional[str]:
    """
    Нормализация номера паспорта

    Args:
        passport_string (str): Строка с номером паспорта

    Returns:
        Optional[str]: Нормализованный номер паспорта или None
    """
    digits = ''.join(filter(str.isdigit, str(passport_string)))

    # Проверка на минимальную длину (обычно 10 цифр для российского паспорта)
    if len(digits) >= 10:
        return digits

    return None


async def search_by_identifier(identifiers: Dict[str, str], api_client) -> List[Dict[str, Any]]:
    """
    Выполнение поиска по стабильным идентификаторам

    Args:
        identifiers (Dict[str, str]): Словарь с идентификаторами
        api_client: Экземпляр API клиента

    Returns:
        List[Dict[str, Any]]: Результаты поиска
    """
    all_results = []

    # Получаем event loop
    loop = asyncio.get_event_loop()

    # Приоритезация идентификаторов
    priority_order = ["email", "passport", "vk_id", "login"]

    for id_type in priority_order:
        if id_type in identifiers and identifiers[id_type]:
            # Логируем идентификатор
            logger.info(f"Поиск по идентификатору {id_type}: {identifiers[id_type]}")

            # Выполнение запроса через run_in_executor
            if id_type == "vk_id":
                # Для VK ID используем специальную функцию
                response = await loop.run_in_executor(
                    None,
                    lambda: api_client.search_vk_id(identifiers[id_type])
                )
            else:
                # Для остальных идентификаторов используем общую функцию
                response = await loop.run_in_executor(
                    None,
                    lambda: api_client.make_request(query=identifiers[id_type])
                )

            # Обработка результатов
            if "error" not in response and "List" in response:
                for db_name, db_info in response["List"].items():
                    if db_name != "No results found" and "Data" in db_info and db_info["Data"]:
                        # Добавляем источник к каждой записи
                        for record in db_info["Data"]:
                            record["_source_db"] = db_name
                            record["_response_key"] = f"by_{id_type}"
                            all_results.append(record)

    # Убираем дубликаты
    unique_results = remove_duplicates(all_results)

    return unique_results

def remove_duplicates(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Удаление дубликатов из результатов

    Args:
        results (List[Dict[str, Any]]): Список результатов

    Returns:
        List[Dict[str, Any]]: Список без дубликатов
    """
    if not results:
        return []

    unique_ids = set()
    unique_results = []

    for result in results:
        # Создание уникального идентификатора для записи
        result_id = create_result_id(result)

        if result_id not in unique_ids:
            unique_ids.add(result_id)
            unique_results.append(result)

    return unique_results


def create_result_id(result: Dict[str, Any]) -> str:
    """
    Создание уникального идентификатора для записи

    Args:
        result (Dict[str, Any]): Запись

    Returns:
        str: Уникальный идентификатор
    """
    id_parts = []

    # Используем ключевые поля для создания идентификатора
    for field in ["name", "surname", "first_name", "firstname", "patronymic", "birth_date", "email", "phone"]:
        if field in result:
            id_parts.append(f"{field}:{result[field]}")

    # Если нет ключевых полей, используем все поля
    if not id_parts:
        for field, value in result.items():
            if not field.startswith("_") and value:  # Исключаем служебные поля
                id_parts.append(f"{field}:{value}")

    return "|".join(id_parts)


def merge_search_results(first_stage: List[Dict[str, Any]], second_stage: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Объединение результатов первого и второго этапа поиска

    Args:
        first_stage (List[Dict[str, Any]]): Результаты первого этапа
        second_stage (List[Dict[str, Any]]): Результаты второго этапа

    Returns:
        List[Dict[str, Any]]: Объединенные результаты
    """
    # Копируем результаты первого этапа
    merged = first_stage.copy()

    # Добавляем результаты второго этапа, избегая дубликатов
    merged.extend(second_stage)

    # Убираем дубликаты
    return remove_duplicates(merged)


def score_phones(results: List[Dict[str, Any]], name_data: Dict[str, str], birth_date: str) -> List[Dict[str, Any]]:
    """
    Скоринг телефонов из результатов поиска

    Args:
        results (List[Dict[str, Any]]): Результаты поиска
        name_data (Dict[str, str]): Данные имени
        birth_date (str): Дата рождения

    Returns:
        List[Dict[str, Any]]: Список оцененных телефонов
    """
    all_phones = []

    for record in results:
        # Ищем все поля с телефонами
        for field_name, field_value in record.items():
            field_lower = field_name.lower()

            if "phone" in field_lower or "телефон" in field_lower or "тел" in field_lower or "mobile" in field_lower:
                if field_value:
                    # Нормализуем телефон
                    phone = normalize_phone(str(field_value))

                    if phone and is_valid_phone(phone):
                        # Оцениваем телефон
                        phone_score = calculate_phone_score(phone, record, name_data, birth_date)

                        # Проверяем, нет ли уже такого телефона
                        existing_phone = next((p for p in all_phones if p["phone"] == phone), None)

                        if existing_phone:
                            # Если такой телефон уже есть, обновляем оценку, если новая выше
                            if phone_score > existing_phone["score"]:
                                existing_phone["score"] = phone_score
                                existing_phone["source"] = record
                        else:
                            # Добавляем новый телефон
                            all_phones.append({
                                "phone": phone,
                                "score": phone_score,
                                "source": record
                            })

    # Сортировка телефонов по убыванию оценки
    all_phones.sort(key=lambda x: x["score"], reverse=True)

    return all_phones


def normalize_phone(phone: str) -> Optional[str]:
    """
    Нормализация телефонного номера

    Args:
        phone (str): Строка с телефоном

    Returns:
        Optional[str]: Нормализованный телефон или None
    """
    # Удаляем все нецифровые символы
    digits = ''.join(filter(str.isdigit, phone))

    # Проверяем минимальную длину
    if len(digits) < 10:
        return None

    # Если номер начинается с 8, заменяем на 7 (для России)
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]

    # Обеспечиваем, что номер начинается с 7 для России
    if len(digits) == 10 and not digits.startswith('7'):
        digits = '7' + digits

    return digits


def is_valid_phone(phone: str) -> bool:
    """
    Проверка валидности телефона

    Args:
        phone (str): Телефонный номер

    Returns:
        bool: True, если телефон валиден
    """
    if not phone:
        return False

    # Проверка минимальной длины (российский номер: 11 цифр)
    if len(phone) < 10:
        return False

    # Проверка, что это только цифры
    if not phone.isdigit():
        return False

    # Для России проверяем префикс 7
    if len(phone) == 11 and not (phone.startswith('7') or phone.startswith('8')):
        return False

    return True


def detect_phone_type(phone: str) -> str:
    """
    Определение типа телефона

    Args:
        phone (str): Телефонный номер

    Returns:
        str: Тип телефона (mobile, landline, voip или unknown)
    """
    if not phone or len(phone) < 10:
        return "unknown"

    # Предполагаем российский номер
    if phone.startswith("79") or phone.startswith("89"):
        return "mobile"
    elif phone.startswith("7495") or phone.startswith("7499") or phone.startswith("7812"):
        return "landline"
    elif phone.startswith("7800") or phone.startswith("8800"):
        return "tollfree"

    # По умолчанию считаем мобильным, если начинается с 7
    if phone.startswith("7"):
        return "mobile"

    return "unknown"


def calculate_phone_score(phone: str, source_record: Dict[str, Any], name_data: Dict[str, str],
                          birth_date: str) -> float:
    """
    Расчет оценки для телефона

    Args:
        phone (str): Телефонный номер
        source_record (Dict[str, Any]): Запись-источник
        name_data (Dict[str, str]): Данные имени
        birth_date (str): Дата рождения

    Returns:
        float: Оценка телефона (0-100)
    """
    # Базовая оценка начинается с 50
    score = 50

    # Фактор 1: Валидность телефона (0-20 баллов)
    if is_valid_phone(phone):
        score += 20

    # Фактор 2: Тип телефона (0-15 баллов)
    phone_type = detect_phone_type(phone)
    if phone_type == "mobile":
        score += 15
    elif phone_type == "landline":
        score += 10
    elif phone_type == "tollfree":
        score += 5

    # Фактор 3: Соответствие данным источника (0-15 баллов)
    name_match_score = 0

    # Проверяем соответствие фамилии
    surname_similarity = 0
    for field_name, field_value in source_record.items():
        field_lower = field_name.lower()
        if "фамилия" in field_lower or "surname" in field_lower or "lastname" in field_lower:
            if field_value:
                surname_similarity = calculate_similarity(str(field_value).lower(), name_data["surname"])
                break

    # Проверяем соответствие имени
    first_name_similarity = 0
    if name_data["first_name"]:
        for field_name, field_value in source_record.items():
            field_lower = field_name.lower()
            if "имя" in field_lower or "firstname" in field_lower or "first_name" in field_lower:
                if field_value:
                    first_name_similarity = calculate_similarity(str(field_value).lower(), name_data["first_name"])
                    break

    # Вычисляем итоговый балл соответствия имени
    if surname_similarity > 0 or first_name_similarity > 0:
        name_match_score = ((surname_similarity * 0.6) + (first_name_similarity * 0.4)) * 15

    score += name_match_score

    # Фактор 4: Совпадение даты рождения (0-10 баллов)
    for field_name, field_value in source_record.items():
        field_lower = field_name.lower()
        if "birth" in field_lower or "рождения" in field_lower or "dob" in field_lower:
            if field_value:
                # Точное совпадение даты
                if birth_date == str(field_value) or convert_date_format(str(field_value)) == birth_date:
                    score += 10
                    break
                # Частичное совпадение (только год и месяц)
                elif birth_date[:7] == str(field_value)[:7] or birth_date[:7] == convert_date_format(str(field_value))[
                                                                                 :7]:
                    score += 7
                    break
                # Частичное совпадение (только год)
                elif birth_date[:4] == str(field_value)[:4] or birth_date[:4] == convert_date_format(str(field_value))[
                                                                                 :4]:
                    score += 5
                    break

    # Фактор 5: Качество базы данных (0-5 баллов)
    if "_source_db" in source_record:
        source_db = source_record["_source_db"]

        # Приоритетные базы данных
        priority_dbs = {
            "Gosuslugi 2024": 5,
            "BolshayaPeremena": 5,
            "AlfaBank 2023 v2": 4,
            "ScanTour.ru": 4,
            "Resh.Edu": 3,
            "ProPostuplenie.ru": 3,
            "Dobro.ru": 3,
            "LeaderID": 3,
            "TrudVsem.ru": 2
        }

        if source_db in priority_dbs:
            score += priority_dbs[source_db]

    # Фактор 6: Оценка совпадения из первого этапа (0-5 баллов)
    if "_match_score" in source_record:
        match_score = source_record["_match_score"]
        if match_score >= 75:
            score += 5
        elif match_score >= 60:
            score += 3
        elif match_score >= 50:
            score += 1

    # Нормализация оценки до диапазона 0-100
    score = min(max(score, 0), 100)

    return score


def select_best_phone(scored_phones: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Выбор лучшего телефона по оценке

    Args:
        scored_phones (List[Dict[str, Any]]): Список оцененных телефонов

    Returns:
        Optional[Dict[str, Any]]: Лучший телефон или None
    """
    if not scored_phones:
        return None

    # Выбор телефона с максимальной оценкой
    best_phone = scored_phones[0]

    # Проверка, что оценка достаточно высокая (минимальный порог)
    if best_phone["score"] < 60:
        logger.warning(f"Лучший телефон имеет низкую оценку: {best_phone['score']}, что ниже порога 60")

        # Если есть другие телефоны, логируем их для отладки
        if len(scored_phones) > 1:
            logger.info(f"Другие найденные телефоны: {[p['phone'] for p in scored_phones[1:]]}")

    return best_phone


async def search_phone_by_name_and_birth_date(name: str, birth_date: str, api_client) -> Dict[str, Any]:
    """
    Двухэтапный поиск телефона по имени и дате рождения
    """
    # Нормализация запроса
    name_parts = name.split()
    surname = name_parts[0] if name_parts else ""
    firstname = name_parts[1] if len(name_parts) > 1 else ""

    # Данные запроса для оценки уверенности
    query_data = {
        "surname": surname,
        "name": firstname,
        "birth_date": birth_date,
        "full_query": f"{name} {birth_date}"
    }

    # Этап 1: Поиск по ФИО и дате рождения
    try:
        # Получаем event loop
        loop = asyncio.get_event_loop()

        # Выполняем запрос к API
        response = await loop.run_in_executor(
            None,
            lambda: api_client.search_by_name_dob(query_data["full_query"])
        )

        # Добавляем отладочное логирование
        logger.info(f"Получен ответ API размером: {len(str(response))} байт")

        # Извлекаем телефоны
        stage1_phones = extract_phones_from_api_response(response)
        logger.info(f"Извлечено {len(stage1_phones)} телефонов: {stage1_phones[:5]}")

        # Если найдены телефоны, конвертируем их в требуемый формат
        phone_entries = []
        for phone in stage1_phones:
            # Добавляем каждый номер как словарь с необходимыми полями
            phone_entries.append({
                "phone": phone,
                "priority": 5,  # Значение приоритета по умолчанию
                "confirmed_count": 0,
                "source": "direct_extract"
            })

        # Если найдено несколько телефонов, посчитаем, сколько раз каждый встречается
        phone_counts = {}
        for entry in phone_entries:
            phone = entry["phone"]
            if phone in phone_counts:
                phone_counts[phone] += 1
            else:
                phone_counts[phone] = 1

        # Дополним информацию о подтверждениях
        for entry in phone_entries:
            entry["confirmed_count"] = phone_counts[entry["phone"]] - 1

        # Если есть телефоны с высоким приоритетом (8+), возвращаем лучший
        high_priority_phones = [p for p in phone_entries if p["priority"] >= 8]
        if high_priority_phones:
            best_phone = high_priority_phones[0]
            confidence = evaluate_phone_confidence(best_phone, query_data)

            if confidence >= 0.8:  # Высокая уверенность
                return {
                    "phones": [best_phone["phone"]],
                    "primary_phone": best_phone["phone"],
                    "method": "priority_database",
                    "confidence": confidence,
                    "source": best_phone["source"]
                }

        # Этап 2: Если нет уверенного результата, ищем email для второго запроса
        emails = extract_emails_from_response(response)

        if emails:
            # Выполняем запрос по email
            # Выполняем запрос по email
            email_response = await loop.run_in_executor(
                None,
                lambda: api_client.make_request(query=emails[0])
            )

            # Извлекаем телефоны из второго запроса
            stage2_phones_raw = extract_phones_from_api_response(email_response)
            logger.info(f"Извлечено {len(stage2_phones_raw)} телефонов из email-запроса")

            # Конвертируем в нужный формат
            stage2_phones = []
            for phone in stage2_phones_raw:
                stage2_phones.append({
                    "phone": phone,
                    "priority": 7,  # Более высокий приоритет для email
                    "confirmed_count": 0,
                    "source": {
                        "_source_db": "email_search"
                    }
                })

            # Обновляем счетчики подтверждений для телефонов из первого этапа
            for s1_entry in phone_entries:
                for s2_entry in stage2_phones:
                    if s1_entry["phone"] == s2_entry["phone"]:
                        s1_entry["confirmed_count"] = s1_entry.get("confirmed_count", 0) + 1
                        s2_entry["confirmed_by_stage1"] = True

            # Объединяем телефоны из обоих этапов
            all_phones = phone_entries + [p for p in stage2_phones if
                                          not any(p["phone"] == s1p["phone"] for s1p in phone_entries)]
            # Сортируем по приоритету и подтверждениям
            all_phones.sort(key=lambda x: (x.get("confirmed_count", 0), x["priority"]), reverse=True)

            if all_phones:
                best_phone = all_phones[0]
                confidence = evaluate_phone_confidence(best_phone, query_data)

                return {
                    "phones": [p["phone"] for p in all_phones[:3]],  # Топ-3 телефона
                    "primary_phone": best_phone["phone"],
                    "method": "two_stage_search",
                    "confidence": confidence,
                    "source": best_phone["source"]
                }

        # Если даже после второго этапа ничего не найдено
        if phone_entries:
            # Возвращаем лучший из имеющихся телефонов
            best_phone = phone_entries[0]
            confidence = evaluate_phone_confidence(best_phone, query_data)

            return {
                "phones": [p["phone"] for p in phone_entries[:3]],
                "primary_phone": best_phone["phone"],
                "method": "best_effort",
                "confidence": confidence,
                "source": best_phone["source"]
            }

        return {
            "phones": [],
            "method": "no_results",
            "confidence": 0.0
        }

    except Exception as e:
        logger.error(f"Ошибка при выполнении поиска: {e}")
        return {"error": f"Ошибка при выполнении поиска: {str(e)}"}