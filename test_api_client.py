#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тестовый скрипт для проверки работы с API
"""

import os
import json
import argparse
import time
from pathlib import Path
from datetime import datetime

import requests

# Настройки API по умолчанию
DEFAULT_API_URL = "https://leakosintapi.com/"


def configure_parser():
    """
    Настройка парсера аргументов командной строки

    Returns:
        argparse.ArgumentParser: Настроенный парсер
    """
    parser = argparse.ArgumentParser(description="Тестовый клиент API LeakOSINT")

    # Основные параметры
    parser.add_argument("--token", "-t", help="API токен", required=True)
    parser.add_argument("--query", "-q", help="Запрос (VK ID)")
    parser.add_argument("--url", "-u", help=f"URL API (по умолчанию: {DEFAULT_API_URL})", default=DEFAULT_API_URL)

    # Дополнительные параметры
    parser.add_argument("--lang", "-l", help="Язык результатов (по умолчанию: ru)", default="ru")
    parser.add_argument("--limit", "-m", help="Лимит поиска (по умолчанию: 100)", type=int, default=100)
    parser.add_argument("--type", help="Тип ответа: json, short, html (по умолчанию: json)", default="json")

    # Параметры вывода
    parser.add_argument("--output", "-o", help="Путь для сохранения результатов")
    parser.add_argument("--verbose", "-v", help="Подробный вывод", action="store_true")

    return parser


def make_api_request(url, token, query, lang="ru", limit=100, result_type="json"):
    """
    Отправка запроса к API

    Args:
        url (str): URL API
        token (str): API токен
        query (str): Запрос (VK ID)
        lang (str): Язык результатов
        limit (int): Лимит поиска
        result_type (str): Тип ответа

    Returns:
        dict: Ответ API
    """
    print(f"Отправка запроса к API: {url}")
    print(f"Запрос: {query}")

    # Подготовка параметров запроса
    params = {
        "token": token,
        "request": query,
        "lang": lang,
        "limit": limit,
        "type": result_type
    }

    # Засекаем время
    start_time = time.time()

    try:
        # Отправка запроса
        response = requests.post(url, json=params)

        # Время выполнения запроса
        execution_time = time.time() - start_time

        print(f"Статус ответа: {response.status_code}")
        print(f"Время выполнения: {execution_time:.2f} секунд")

        # Если ответ успешный
        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError:
                print("Ошибка при разборе JSON ответа")
                print(f"Текст ответа: {response.text[:500]}...")
                return None
        else:
            print(f"Ошибка API: {response.status_code}")
            print(f"Текст ответа: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при отправке запроса: {e}")
        return None


def save_result(result, query, output_path=None):
    """
    Сохранение результата в файл

    Args:
        result (dict): Результат запроса
        query (str): Запрос
        output_path (str, optional): Путь для сохранения

    Returns:
        str: Путь к созданному файлу
    """
    # Если путь не указан, создаем файл в текущей директории
    if not output_path:
        output_dir = Path("results")
        output_dir.mkdir(exist_ok=True)

        # Генерируем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"vk_id_{query}_{timestamp}.json"
        output_path = output_dir / filename

    # Сохраняем результат в файл
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Результат сохранен в файл: {output_path}")
    return str(output_path)


def print_result_summary(result):
    """
    Вывод краткой информации о результате

    Args:
        result (dict): Результат запроса
    """
    if "error" in result:
        print(f"Ошибка: {result['error']}")
        return

    if "List" not in result:
        print("Нет результатов")
        return

    print("\nНайденные данные:")
    for db_name, db_info in result["List"].items():
        print(f"\n== База данных: {db_name} ==")

        if "InfoLeak" in db_info:
            print(f"Информация: {db_info['InfoLeak']}")

        if db_name == "No results found" or "Data" not in db_info or not db_info["Data"]:
            print("Данные не найдены")
            continue

        print(f"Количество записей: {len(db_info['Data'])}")

        # Выводим первую запись как пример
        if db_info["Data"]:
            print("\nПример записи:")
            for field_name, field_value in db_info["Data"][0].items():
                value = str(field_value) if field_value is not None else "NULL"
                print(f"  {field_name}: {value}")


def estimate_request_cost(query, limit=100):
    """
    Оценка стоимости запроса

    Args:
        query (str): Запрос
        limit (int): Лимит поиска

    Returns:
        float: Стоимость запроса в долларах
    """
    import math

    # Подсчет слов в запросе (упрощенно)
    words = len([w for w in query.split() if len(w) >= 4 or (w.isdigit() and len(w) >= 6)])

    # Определение сложности запроса
    if words <= 1:
        complexity = 1
    elif words == 2:
        complexity = 5
    elif words == 3:
        complexity = 16
    else:  # words > 3
        complexity = 40

    # Расчет стоимости по формуле: (5 + sqrt(Limit * Complexity)) / 5000
    cost = (5 + math.sqrt(limit * complexity)) / 5000

    return cost


def main():
    """Основная функция"""
    parser = configure_parser()
    args = parser.parse_args()

    # Проверка токена
    if not args.token:
        print("Ошибка: API токен обязателен. Используйте параметр --token или -t")
        return

    # Если запрос не указан, запрашиваем его интерактивно
    query = args.query
    if not query:
        query = input("Введите VK ID для поиска: ")

    # Оценка стоимости запроса
    cost = estimate_request_cost(query, args.limit)
    print(f"Оценочная стоимость запроса: ${cost:.6f}")

    confirmation = input("Продолжить? (y/n): ")
    if confirmation.lower() != 'y':
        print("Операция отменена")
        return

    # Выполнение запроса
    result = make_api_request(
        url=args.url,
        token=args.token,
        query=query,
        lang=args.lang,
        limit=args.limit,
        result_type=args.type
    )

    if result:
        # Вывод краткой информации о результате
        print_result_summary(result)

        # Сохранение результата в файл
        if args.output:
            save_result(result, query, args.output)
        else:
            save_result(result, query)

        # Вывод полного результата в подробном режиме
        if args.verbose:
            print("\nПолный результат:")
            print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()