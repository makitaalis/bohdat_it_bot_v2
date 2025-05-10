#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Клиент для работы с API LeakOSINT
"""
import datetime
import time
import traceback

import json
import requests
from typing import Dict, Any, List, Union, Optional, Tuple
import re

from config import (
    API_URL,
    LEAKOSINT_API_TOKEN,
    API_DEFAULT_LANG,
    API_DEFAULT_LIMIT,
    API_DEFAULT_TYPE
)
from logger import logger, log_api_request, log_api_response
from database import db


# Приоритетные базы данных
SOURCE_PRIORITY = {
    "Gosuslugi 2024": 10,
    "BolshayaPeremena": 9,
    "AlfaBank 2023 v2": 8,
    "ScanTour.ru": 7,
    "Resh.Edu": 6,
    "Dobro.ru": 5,
    "TrudVsem.ru": 4,
    # Можно добавить другие базы при необходимости
}



class APIClient:
    """Клиент для работы с API LeakOSINT"""

    def __init__(self, token: str = LEAKOSINT_API_TOKEN,
                 url: str = API_URL,
                 default_lang: str = API_DEFAULT_LANG,
                 default_limit: int = API_DEFAULT_LIMIT,
                 default_type: str = API_DEFAULT_TYPE):
        """
        Инициализация клиента API

        Args:
            token (str): API токен
            url (str): URL API
            default_lang (str): Язык по умолчанию
            default_limit (int): Лимит поиска по умолчанию
            default_type (str): Тип ответа по умолчанию
        """
        self.token = token
        self.url = url
        self.default_lang = default_lang
        self.default_limit = default_limit
        self.default_type = default_type

        # Последнее время запроса для контроля частоты
        self.last_request_time = 0

        # Добавляем регулярные выражения для проверки форматов VK ID
        self.vk_id_patterns = [
            re.compile(r'^id\d+$'),  # формат id123456
            re.compile(r'^\d+$'),  # просто числовой формат
            re.compile(r'^vk\.com\/id\d+$'),  # формат vk.com/id123456
            re.compile(r'^vk\.com\/[a-zA-Z0-9_.]+$'),  # формат vk.com/username
        ]

    def search_vk_id(self, vk_id: str,
                     lang: str = None,
                     limit: int = None,
                     result_type: str = None,
                     save_json: bool = True) -> Dict[str, Any]:
        """
        Поиск информации по VK ID

        Args:
            vk_id (str): VK ID для поиска
            lang (str, optional): Язык результатов
            limit (int, optional): Лимит поиска
            result_type (str, optional): Тип ответа
            save_json (bool, optional): Сохранять ли JSON-ответ в файл

        Returns:
            Dict[str, Any]: Результаты поиска
        """
        # Форматируем и валидируем запрос
        formatted_id, is_valid = self._format_vk_id(vk_id)
        if not is_valid:
            logger.warning(f"Invalid VK ID format: {vk_id}")
            return {
                "error": "Неверный формат VK ID. Используйте числовой ID или формат 'id123456'"
            }

        query = formatted_id

        # Проверяем кеш перед запросом к API
        cache_key = f"vk:{query}:{lang or self.default_lang}:{limit or self.default_limit}"
        cached_response = db.get_cached_response(cache_key)

        if cached_response:
            # Если ответ из кеша и нужно сохранить JSON, сохраняем его с пометкой
            if save_json:
                from formatter import formatter
                cached_response_with_meta = cached_response.copy()
                cached_response_with_meta["_meta"] = {
                    "source": "cache",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "vk_id": vk_id,
                    "formatted_id": formatted_id
                }
                formatter.save_json_file(cached_response_with_meta, formatted_id)
            return cached_response

        # Если в кеше нет, делаем запрос к API
        response = self.make_request(
            query=query,
            lang=lang,
            limit=limit,
            result_type=result_type
        )

        # Обрабатываем и сохраняем телефонные номера из ответа
        if "error" not in response:
            phone_count = db.extract_and_save_phone_numbers(response, formatted_id)
            logger.info(f"Extracted {phone_count} phone numbers from response for VK ID: {formatted_id}")

        # Сохраняем JSON-ответ от API, если нужно
        if save_json and "error" not in response:
            from formatter import formatter
            response_with_meta = response.copy()
            response_with_meta["_meta"] = {
                "source": "api",
                "timestamp": datetime.datetime.now().isoformat(),
                "vk_id": vk_id,
                "formatted_id": formatted_id
            }
            formatter.save_json_file(response_with_meta, formatted_id)

        # Сохраняем результат в кеш
        db.cache_response(cache_key, response)

        return response

    def search_batch(self, vk_ids: List[str],
                     lang: str = None,
                     limit: int = None,
                     result_type: str = None,
                     save_json: bool = True) -> Dict[str, Any]:
        """
        Поиск информации по нескольким VK ID за один запрос к API

        Args:
            vk_ids (List[str]): Список VK ID для поиска
            lang (str, optional): Язык результатов
            limit (int, optional): Лимит поиска
            result_type (str, optional): Тип ответа
            save_json (bool, optional): Сохранять ли JSON-ответ в файл

        Returns:
            Dict[str, Any]: Результаты поиска
        """
        if not vk_ids:
            return {"error": "Пустой список VK ID"}

        # Форматируем ID и отфильтровываем невалидные
        formatted_ids = []
        original_to_formatted = {}  # Для связи оригинальных ID с форматированными

        for vk_id in vk_ids:
            formatted_id, is_valid = self._format_vk_id(vk_id)
            if is_valid:
                formatted_ids.append(formatted_id)
                original_to_formatted[vk_id] = formatted_id

        if not formatted_ids:
            return {"error": "Нет валидных VK ID в списке"}

        # Генерируем уникальный ключ для кеша на основе всех параметров запроса
        cache_key = f"batch:{','.join(formatted_ids)}:{lang or self.default_lang}:{limit or self.default_limit}"
        cached_response = db.get_cached_response(cache_key)

        if cached_response:
            logger.info(f"Found cached result for batch request with {len(formatted_ids)} IDs")
            if save_json:
                from formatter import formatter
                cached_response_with_meta = cached_response.copy()
                cached_response_with_meta["_meta"] = {
                    "source": "cache",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "batch_size": len(formatted_ids)
                }
                formatter.save_json_file(cached_response_with_meta, f"batch_{len(formatted_ids)}_ids")
            return cached_response

        logger.info(f"Making batch request for {len(formatted_ids)} VK IDs")

        # Создаем строку с ID, разделенными переносом строки вместо списка
        batch_query = "\n".join(formatted_ids)

        # Отправляем пакетный запрос к API используя строку с разделителями
        response = self.make_request(
            query=batch_query,  # Отправляем строку с ID, разделенными \n
            lang=lang,
            limit=limit,
            result_type=result_type
        )

        # Логируем полный ответ для отладки при ошибке
        if "error" in response:
            logger.debug(f"Full batch request error response: {response}")

        # Обрабатываем и сохраняем телефонные номера из ответа
        if "error" not in response:
            # Для пакетного запроса мы не можем сохранить телефоны по отдельным ID,
            # т.к. не знаем структуру ответа. Возможно, потребуется специальная обработка.
            logger.info(f"Received successful batch response for {len(formatted_ids)} VK IDs")

            # Кешируем результат
            db.cache_response(cache_key, response)

            # Сохраняем JSON-ответ, если нужно
            if save_json:
                from formatter import formatter
                response_with_meta = response.copy()
                response_with_meta["_meta"] = {
                    "source": "api",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "batch_size": len(formatted_ids),
                    "vk_ids": formatted_ids,
                    "query_format": "newline_separated"  # Отмечаем формат запроса
                }
                formatter.save_json_file(response_with_meta, f"batch_{len(formatted_ids)}_ids")
        else:
            logger.error(f"Batch request error: {response.get('error')}")

            # Если ошибка связана с форматом или неизвестна, пытаемся обработать по одному ID
            if "External server unavailable" in response.get('error', ''):
                logger.warning(
                    "External server unavailable error detected. Consider reducing batch size or retrying later.")

        return response

    def search_phone(self, phone: str) -> Dict[str, Any]:
        """
        Поиск информации по номеру телефона

        Args:
            phone (str): Номер телефона

        Returns:
            Dict[str, Any]: Результаты поиска из локальной базы
        """
        # Нормализуем телефон
        normalized_phone = ''.join(c for c in phone if c.isdigit())

        if len(normalized_phone) < 7:
            return {
                "error": "Номер телефона должен содержать не менее 7 цифр"
            }

        # Ищем в локальной базе
        results = db.search_phone_number(normalized_phone)

        if not results:
            return {
                "List": {
                    "No results found": {
                        "InfoLeak": "По вашему запросу ничего не найдено",
                        "Data": []
                    }
                }
            }

        # Формируем ответ в формате, похожем на API
        response = {
            "List": {
                "LocalDatabase": {
                    "InfoLeak": f"Найдено {len(results)} записей в локальной базе данных",
                    "Data": []
                }
            }
        }

        for result in results:
            entry = {
                "VkID": result.get("vk_id", ""),
                "FullName": result.get("full_name", ""),
                "Source": result.get("source", ""),
                "Phone": normalized_phone
            }
            response["List"]["LocalDatabase"]["Data"].append(entry)

        return response

    def _format_vk_id(self, vk_id: str) -> Tuple[str, bool]:
        """
        Форматирует и валидирует VK ID

        Args:
            vk_id (str): VK ID в различных форматах

        Returns:
            Tuple[str, bool]: Отформатированный ID и флаг валидности
        """
        if not vk_id:
            return "", False

        # Очищаем от пробелов и приводим к нижнему регистру
        cleaned_id = vk_id.strip().lower()

        # Проверяем на соответствие шаблонам
        for pattern in self.vk_id_patterns:
            if pattern.match(cleaned_id):
                # Извлекаем числовую часть ID
                if cleaned_id.startswith("vk.com/id"):
                    numeric_id = cleaned_id.split("/id")[1]
                elif cleaned_id.startswith("id"):
                    numeric_id = cleaned_id[2:]
                elif cleaned_id.startswith("vk.com/"):
                    # Для имен пользователей возвращаем без vk.com/
                    # Но отмечаем как невалидный, так как нам нужны только числовые ID
                    return cleaned_id.split("/")[1], False
                else:
                    numeric_id = cleaned_id

                # Проверяем, что ID состоит из 9 цифр
                if numeric_id.isdigit() and len(numeric_id) == 9:
                    return numeric_id, True
                else:
                    # Возвращаем ID, но отмечаем как невалидный
                    return numeric_id, False

        # Если не соответствует ни одному шаблону
        return vk_id, False

    def make_request(self, query: Union[str, List[str]],
                     lang: str = None,
                     limit: int = None,
                     result_type: str = None,
                     bot_name: str = None,
                     max_retries: int = 3) -> Dict[str, Any]:
        """
        Отправка запроса к API с механизмом повторных попыток

        Args:
            query (Union[str, List[str]]): Строка запроса или список запросов
            lang (str, optional): Язык результатов
            limit (int, optional): Лимит поиска
            result_type (str, optional): Тип ответа
            bot_name (str, optional): Имя бота
            max_retries (int, optional): Максимальное количество повторных попыток

        Returns:
            Dict[str, Any]: Ответ API
        """
        # Подготовка параметров запроса
        params = {
            "token": self.token,
            "request": query,
            "lang": lang or self.default_lang,
            "limit": limit or self.default_limit,
            "type": result_type or self.default_type
        }

        # Добавляем bot_name, если он указан
        if bot_name:
            params["bot_name"] = bot_name

        # Логируем запрос
        log_api_request(query, params)

        # Повторные попытки с экспоненциальной задержкой
        retry = 0
        last_error = None

        while retry <= max_retries:
            try:
                # Контроль частоты запросов (не более 1 запроса в секунду)
                self._rate_limit()

                start_time = time.time()

                # Отправка запроса к API с увеличенным таймаутом для пакетных запросов
                timeout = 60 if isinstance(query, list) and len(query) > 10 else 30
                response = requests.post(self.url, json=params, timeout=timeout)

                # Засекаем время запроса для контроля частоты
                self.last_request_time = time.time()

                # Логируем ответ
                response_size = len(response.content)
                log_api_response(query, response.status_code, response_size)

                # Обработка ответа
                if response.status_code == 200:
                    try:
                        data = response.json()

                        # НОВЫЙ КОД: Проверка наличия ошибки в ответе API
                        if "Error code" in data and int(data.get("Error code", 0)) != 0:
                            error_msg = f"API error {data.get('Error code')}: {data.get('Status', 'Unknown error')}"
                            logger.error(error_msg)
                            # Возвращаем данные как есть для обработки ошибки вызывающей стороной
                            return data

                        return data
                    except json.JSONDecodeError as e:
                        last_error = e
                        logger.error(f"Failed to parse API response: {response.text[:500]}... Error: {e}")
                        # Для JSON-ошибки нет смысла повторять запрос
                        return {"error": f"Ошибка при обработке ответа API: неверный формат JSON"}
                elif response.status_code == 500 or response.status_code == 503:
                    # Серверная ошибка, пробуем повторить
                    error_msg = f"Ошибка API: {response.status_code}"
                    try:
                        error_json = response.json()
                        if "error" in error_json:
                            error_msg += f" - {error_json['error']}"
                    except:
                        if response.text:
                            error_msg += f" - {response.text[:100]}"

                    logger.warning(f"API server error (retry {retry + 1}/{max_retries}): {error_msg}")
                    last_error = error_msg

                    # Экспоненциальная задержка перед повторной попыткой
                    delay = 2 ** retry
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    # Другая ошибка, не связанная с доступностью сервера
                    error_msg = f"Ошибка API: {response.status_code}"
                    try:
                        error_json = response.json()
                        if "error" in error_json:
                            error_msg += f" - {error_json['error']}"
                    except:
                        if response.text:
                            error_msg += f" - {response.text[:100]}"

                    logger.error(f"API request failed: {error_msg}")
                    return {"error": error_msg}

            except requests.exceptions.Timeout:
                logger.warning(f"API request timeout (retry {retry + 1}/{max_retries}) for query: {query}")
                last_error = "Тайм-аут при подключении к API."

                # Экспоненциальная задержка перед повторной попыткой
                delay = 2 ** retry
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

            except requests.exceptions.ConnectionError:
                logger.warning(f"API connection error (retry {retry + 1}/{max_retries}) for query: {query}")
                last_error = "Ошибка соединения с API."

                # Экспоненциальная задержка перед повторной попыткой
                delay = 2 ** retry
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

            except requests.exceptions.RequestException as e:
                logger.error(f"API request exception: {e}")
                return {"error": f"Ошибка соединения с API: {str(e)}"}

            finally:
                # Записываем информацию о времени выполнения запроса
                processing_time = time.time() - start_time
                logger.debug(f"API request processing time: {processing_time:.2f} seconds")

            retry += 1

        # Если мы здесь, значит все попытки исчерпаны
        logger.error(f"All {max_retries} retries failed for query: {query}")
        return {"error": f"Все попытки запроса к API не удались. Последняя ошибка: {last_error}"}

    def _rate_limit(self):
        """
        Контроль частоты запросов (не более 1 запроса в секунду)
        """
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < 1.0:
            # Если прошло меньше секунды, ждем
            sleep_time = 1.0 - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

    def calculate_request_cost(self, query: str, limit: int = None) -> float:
        """
        Расчет стоимости запроса

        Args:
            query (str): Строка запроса
            limit (int, optional): Лимит поиска

        Returns:
            float: Стоимость запроса в долларах
        """
        # Используем лимит по умолчанию, если не указан
        actual_limit = limit or self.default_limit

        # Подсчет слов в запросе с учетом правил из документации
        words = self._count_words(query)

        # Определение сложности запроса
        if words == 1:
            complexity = 1
        elif words == 2:
            complexity = 5
        elif words == 3:
            complexity = 16
        else:  # words > 3
            complexity = 40

        # Расчет стоимости по формуле: (5 + sqrt(Limit * Complexity)) / 5000
        import math
        cost = (5 + math.sqrt(actual_limit * complexity)) / 5000

        return cost

    def _count_words(self, query: str) -> int:
        """
        Подсчет количества слов в запросе по правилам API

        Args:
            query (str): Строка запроса

        Returns:
            int: Количество слов
        """
        if not query:
            return 0

        # Разбиваем запрос на слова
        words = query.split()

        # Фильтруем слова по правилам:
        # - Даты не считаются словами
        # - Строки короче 4 символов не считаются словами
        # - Числа короче 6 символов не считаются словами
        valid_words = []

        for word in words:
            # Пропускаем даты (примитивная проверка)
            if self._is_date(word):
                continue

            # Пропускаем строки короче 4 символов
            if len(word) < 4:
                continue

            # Пропускаем числа короче 6 символов
            if word.isdigit() and len(word) < 6:
                continue

            valid_words.append(word)

        return len(valid_words)

    def _is_date(self, text: str) -> bool:
        """
        Проверка, является ли строка датой

        Args:
            text (str): Строка для проверки

        Returns:
            bool: True, если строка похожа на дату
        """
        if not text:
            return False

        # Простая проверка на формат даты (можно улучшить)
        import re
        date_patterns = [
            r'\d{2}\.\d{2}\.\d{4}',  # DD.MM.YYYY
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}'  # DD/MM/YYYY
        ]

        for pattern in date_patterns:
            if re.match(pattern, text):
                return True

        return False

    def search_by_name_dob(self, query: str,
                           lang: str = None,
                           limit: int = None,
                           result_type: str = None,
                           save_json: bool = True) -> Dict[str, Any]:
        """
        Поиск информации по ФИО и дате рождения

        Поддерживает различные форматы даты и корректно обрабатывает кириллицу.

        Args:
            query (str): Запрос в формате "Фамилия Имя ДД.ММ.ГГГГ"
            lang (str, optional): Язык результатов
            limit (int, optional): Лимит поиска
            result_type (str, optional): Тип ответа
            save_json (bool, optional): Сохранять ли JSON-ответ в файл

        Returns:
            Dict[str, Any]: Результаты поиска
        """
        # Проверяем кеш перед запросом к API
        cache_key = f"name_dob:{query}:{lang or self.default_lang}:{limit or self.default_limit}"
        cached_response = db.get_cached_response(cache_key)

        # Если ответ найден в кеше, возвращаем его
        if cached_response:
            logger.info(f"Найден кешированный ответ для запроса: {query}")
            # Добавляем метаданные для отладки
            if save_json:
                from formatter import formatter
                cached_response_with_meta = cached_response.copy()
                cached_response_with_meta["_meta"] = {
                    "source": "cache",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "query": query,
                    "search_type": "name_dob"
                }
                formatter.save_json_file(cached_response_with_meta, f"name_dob_search_{query.replace(' ', '_')}")
            return cached_response

        # Стандартизация и подготовка запроса
        parts = query.split()
        original_query = query
        processed_query = query

        # Подробное логирование
        logger.info(f"Выполнение поиска по ФИО и дате рождения: {query}")

        # Отправка запроса к API
        try:
            logger.info(f"Отправка запроса к API: {processed_query}")
            response = self.make_request(
                query=processed_query,
                lang=lang,
                limit=limit,
                result_type=result_type
            )

            # Проверка наличия результатов
            if "List" in response and len(response["List"]) > 0:
                db_names = list(response["List"].keys())
                logger.info(f"Получен ответ от API. Найдены базы данных: {db_names}")

                # Детальное логирование для базы BolshayaPeremena (часто там находятся телефоны)
                if "BolshayaPeremena" in response["List"]:
                    bp_data = response["List"]["BolshayaPeremena"]
                    if "Data" in bp_data and isinstance(bp_data["Data"], list):
                        logger.info(f"Найдено {len(bp_data['Data'])} записей в базе BolshayaPeremena")

                        # Ищем телефоны в данных
                        for record in bp_data["Data"][:5]:  # Проверяем первые 5 записей
                            phone_fields = []
                            for field_name, field_value in record.items():
                                if "телефон" in field_name.lower() or "phone" in field_name.lower():
                                    phone_fields.append(f"{field_name}: {field_value}")
                            if phone_fields:
                                logger.info(f"Найдены поля с телефонами в записи: {phone_fields}")

                # Сохраняем найденную информацию в кеш
                db.cache_response(cache_key, response)

            else:
                logger.warning(f"Ответ получен, но не содержит данных. Keys: {list(response.keys())}")

            return response

        except Exception as e:
            logger.error(f"Ошибка при поиске по ФИО и дате рождения: {e}")
            return {"error": f"Ошибка при запросе к API: {str(e)}"}


def extract_phones_recursive(response_data, target_vk_id=None) -> List[str]:
    """
    Улучшенная универсальная функция для рекурсивного извлечения телефонов из любой части API-ответа.
    С расширенной защитой от ошибок типов данных.

    Args:
        response_data: Данные ответа API (словарь, список, строка или другой тип)
        target_vk_id: Опциональный VK ID для фильтрации результатов

    Returns:
        List[str]: Список телефонных номеров в формате 79XXXXXXXXX
    """
    import re
    phones = set()  # Используем set для уникальности

    # Защита от None
    if response_data is None:
        return []

    def walk(node, path=""):
        """
        Рекурсивный обход всех узлов JSON-структуры с отслеживанием пути и защитой от ошибок типов

        Args:
            node: Текущий узел (может быть словарем, списком, строкой и т.д.)
            path: Путь до текущего узла для отладки
        """
        # Словарь
        if isinstance(node, dict):
            # Сначала проверяем поля, которые могут содержать телефоны
            for key, value in node.items():
                # Защита от нестроковых ключей
                if not isinstance(key, (str, int, float)):
                    continue

                key_str = str(key).lower()

                # Если ключ похож на поле телефона
                if any(phone_field in key_str for phone_field in
                       ["phone", "телефон", "тел", "моб", "mobile", "contact"]):
                    try:
                        # Защита от None
                        if value is None:
                            continue

                        if isinstance(value, (str, int, float)):
                            # Извлекаем только цифры
                            value_str = str(value)
                            digits = ''.join(c for c in value_str if c.isdigit())

                            if len(digits) >= 10:
                                # Обрабатываем разные форматы
                                if digits.startswith('8') and len(digits) == 11:
                                    # Заменяем 8 на 7 для стандартизации
                                    digits = '7' + digits[1:]
                                elif len(digits) == 10 and not digits.startswith('7'):
                                    # Добавляем 7 в начало для 10-значных номеров
                                    digits = '7' + digits

                                # Проверяем, что это действительно российский номер
                                if digits.startswith('7') and len(digits) == 11:
                                    phones.add(digits)
                                    # Логгируем найденный телефон для отладки
                                    logger.debug(f"Найден телефон {digits} в поле {key_str} по пути {path}")
                    except Exception as e:
                        # Игнорируем ошибки при обработке значений, просто логгируем их
                        logger.debug(f"Ошибка при обработке значения для ключа {key_str}: {e}")

            # Затем рекурсивно обходим все значения
            for key, value in node.items():
                try:
                    new_path = f"{path}.{key}" if path else str(key)
                    walk(value, new_path)
                except Exception as e:
                    # Игнорируем ошибки при рекурсивном обходе
                    logger.debug(f"Ошибка при рекурсивном обходе для ключа {key}: {e}")

        # Список
        elif isinstance(node, list):
            # Обходим все элементы списка
            for i, item in enumerate(node):
                try:
                    new_path = f"{path}[{i}]"
                    walk(item, new_path)
                except Exception as e:
                    # Игнорируем ошибки при рекурсивном обходе
                    logger.debug(f"Ошибка при рекурсивном обходе списка по индексу {i}: {e}")

        # Строка или другой примитивный тип
        else:
            try:
                # Для остальных типов данных ищем телефоны в строковом представлении
                node_str = str(node)

                # Используем регулярное выражение для поиска телефонов
                # Ищем: +7/8 код номер с разными разделителями, а также чистые цифры
                phone_patterns = [
                    r'(?:\+?7|8)[\s\-\(\)]*\d{3}[\s\-\(\)]*\d{3}[\s\-\(\)]*\d{2}[\s\-\(\)]*\d{2}',  # +7 (999) 123-45-67
                    r'(?<!\d)[78]\d{10}(?!\d)'  # 79991234567 или 89991234567
                ]

                for pattern in phone_patterns:
                    matches = re.findall(pattern, node_str)
                    for match in matches:
                        # Оставляем только цифры
                        digits = ''.join(c for c in match if c.isdigit())

                        # Стандартизация номера
                        if len(digits) == 11:
                            if digits.startswith('8'):
                                digits = '7' + digits[1:]

                            if digits.startswith('7'):
                                phones.add(digits)
                                logger.debug(f"Найден телефон {digits} через регулярное выражение в пути {path}")
            except Exception as e:
                # Игнорируем ошибки при обработке примитивных типов
                logger.debug(f"Ошибка при обработке примитивного типа: {e}")

    # Начинаем рекурсивный обход с защитой от ошибок
    try:
        walk(response_data)
    except Exception as e:
        logger.error(f"Критическая ошибка при обходе структуры данных: {e}")
        logger.error(traceback.format_exc())

    # Преобразуем set обратно в список
    result = list(phones)
    logger.info(f"Найдено {len(result)} уникальных телефонов в рекурсивном обходе")
    return result


# Создаем экземпляр клиента API
api_client = APIClient()

