#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Анализатор ответов API для LeakOSINT

Этот скрипт выполняет пакетный запрос к API с указанными VK ID
и сохраняет полный ответ в JSON-файл для последующего анализа.

Использование:
python analyze_api_response.py 123456789 223456789 323456789
python analyze_api_response.py --file vk_ids.txt
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
import traceback

# Добавляем путь к проекту в  PYTHONPATH, чтобы импортировать модули проекта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Импортируем нужные модули из проекта
try:
    from config import LEAKOSINT_API_TOKEN, API_URL, API_DEFAULT_LANG, API_DEFAULT_LIMIT, TEMP_DIR
    from api_client import APIClient
except ImportError as e:
    print(f"Ошибка импорта модулей проекта: {e}")
    print("Убедитесь, что скрипт запускается из корня проекта")
    sys.exit(1)


def setup_argparser():
    """Настройка парсера аргументов командной строки"""
    parser = argparse.ArgumentParser(description="Анализатор ответов API для LeakOSINT")

    # Создаем взаимоисключающую группу для источников VK IDs
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids", "-i", nargs="+", help="Список VK ID для анализа")
    group.add_argument("--file", "-f", help="Путь к файлу со списком VK ID (по одному ID в строке)")

    # Другие аргументы
    parser.add_argument("--token", "-t", help=f"API токен (по умолчанию из config.py)")
    parser.add_argument("--lang", "-l", help=f"Язык результатов (по умолчанию: {API_DEFAULT_LANG})",
                        default=API_DEFAULT_LANG)
    parser.add_argument("--limit", "-m", help=f"Лимит поиска (по умолчанию: {API_DEFAULT_LIMIT})", type=int,
                        default=API_DEFAULT_LIMIT)
    parser.add_argument("--output", "-o", help="Путь для сохранения результата (по умолчанию: temp/analysis/)")
    return parser


def load_vk_ids_from_file(file_path):
    """Загрузка VK ID из файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Извлекаем VK ID, очищаем от пробелов и пустых строк
        vk_ids = [line.strip() for line in lines if line.strip()]
        print(f"Загружено {len(vk_ids)} VK ID из файла {file_path}")
        return vk_ids
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")
        sys.exit(1)


def perform_initial_analysis(response, vk_ids):
    """Предварительный анализ структуры ответа API"""
    analysis = {
        "databases": [],
        "potential_phone_fields": set(),
        "potential_id_fields": set(),
        "id_to_record_indices": {},
        "id_occurrences": {}
    }

    if "List" not in response:
        return {"error": "No 'List' key in response"}

    # Для каждого запрошенного VK ID подсчитываем вхождения
    for vk_id in vk_ids:
        analysis["id_occurrences"][vk_id] = 0

    # Для каждой базы данных
    for db_name, db_info in response["List"].items():
        db_analysis = {
            "name": db_name,
            "record_count": 0,
            "has_data": False,
            "fields": set(),
            "sample_records": []  # Сохраняем несколько примеров записей
        }

        if "Data" in db_info and isinstance(db_info["Data"], list):
            db_analysis["has_data"] = True
            data = db_info["Data"]
            db_analysis["record_count"] = len(data)

            # Сохраняем до 5 примеров записей
            for i, record in enumerate(data[:5]):
                if isinstance(record, dict):
                    db_analysis["sample_records"].append(record)

            # Извлекаем все имена полей
            for record in data:
                if isinstance(record, dict):
                    for field_name in record.keys():
                        db_analysis["fields"].add(field_name)

                        # Ищем потенциальные поля с ID и телефонами
                        field_lower = field_name.lower()
                        if any(id_pattern in field_lower for id_pattern in ["vkid", "vk_id", "id", "userid"]):
                            analysis["potential_id_fields"].add(field_name)
                        elif any(phone_pattern in field_lower for phone_pattern in
                                 ["phone", "телефон", "тел", "мобильный"]):
                            analysis["potential_phone_fields"].add(field_name)

            # Поиск конкретных VK ID в базе
            for vk_id in vk_ids:
                record_indices = []

                for i, record in enumerate(data):
                    if not isinstance(record, dict):
                        continue

                    # Первый проход: ищем прямые совпадения в полях
                    vk_id_found = False
                    for field_name, field_value in record.items():
                        if str(field_value) == vk_id:
                            record_indices.append(i)
                            analysis["id_occurrences"][vk_id] += 1
                            vk_id_found = True
                            break

                    # Если совпадение не найдено, ищем как подстроку
                    if not vk_id_found:
                        for field_name, field_value in record.items():
                            if isinstance(field_value, str) and vk_id in field_value:
                                record_indices.append(i)
                                analysis["id_occurrences"][vk_id] += 1
                                break

                if record_indices:
                    if vk_id not in analysis["id_to_record_indices"]:
                        analysis["id_to_record_indices"][vk_id] = {}

                    analysis["id_to_record_indices"][vk_id][db_name] = record_indices

        analysis["databases"].append(db_analysis)

    # Анализ распределения телефонов
    analysis["phone_distribution"] = analyze_phone_distribution(response, vk_ids)

    # Конвертируем set в list для JSON-сериализации
    analysis["potential_phone_fields"] = list(analysis["potential_phone_fields"])
    analysis["potential_id_fields"] = list(analysis["potential_id_fields"])
    for db in analysis["databases"]:
        db["fields"] = list(db["fields"])

    return analysis


def analyze_phone_distribution(response, vk_ids):
    """
    Анализирует распределение телефонных номеров в ответе API
    и пытается определить закономерности их связи с VK ID
    """
    distribution = {
        "phone_count": 0,
        "phone_patterns": [],
        "relative_positions": {}
    }

    if "List" not in response:
        return distribution

    # Собираем все записи с телефонами
    all_phone_records = []

    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        # Ищем телефоны
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                continue

            has_phone = False
            for field_name, field_value in record.items():
                if any(phone_pattern in field_name.lower() for phone_pattern in
                       ["phone", "телефон", "тел", "мобильный"]):
                    if field_value:
                        digits = ''.join(c for c in str(field_value) if c.isdigit())
                        if digits.startswith('79') and len(digits) >= 11:
                            has_phone = True
                            distribution["phone_count"] += 1

                            # Сохраняем запись с телефоном и её индекс
                            all_phone_records.append((i, record, digits, db_name))

                            # Определяем шаблон (структуру поля с телефоном)
                            pattern = {
                                "field_name": field_name,
                                "database": db_name,
                                "example": digits,
                                "count": 1
                            }

                            # Проверяем, есть ли уже такой шаблон
                            pattern_exists = False
                            for p in distribution["phone_patterns"]:
                                if p["field_name"] == field_name and p["database"] == db_name:
                                    p["count"] += 1
                                    pattern_exists = True
                                    break

                            if not pattern_exists:
                                distribution["phone_patterns"].append(pattern)

    # Анализируем относительное расположение телефонов и VK ID
    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        # Для каждого VK ID
        for vk_id in vk_ids:
            # Ищем записи с этим VK ID
            vk_id_indices = []
            for i, record in enumerate(data):
                if not isinstance(record, dict):
                    continue

                for field_name, field_value in record.items():
                    if str(field_value) == vk_id:
                        vk_id_indices.append(i)
                        break

            # Если найдены записи с этим VK ID
            if vk_id_indices:
                # Для каждой записи с телефоном в этой же базе
                phone_records_in_db = [rec for rec in all_phone_records if rec[3] == db_name]

                for phone_idx, phone_record, phone, _ in phone_records_in_db:
                    # Для каждой записи с VK ID вычисляем относительную позицию
                    for vk_idx in vk_id_indices:
                        rel_pos = phone_idx - vk_idx

                        # Добавляем информацию о расположении
                        key = f"{rel_pos}"
                        if key not in distribution["relative_positions"]:
                            distribution["relative_positions"][key] = {
                                "count": 0,
                                "examples": []
                            }

                        distribution["relative_positions"][key]["count"] += 1

                        # Добавляем пример, если их меньше 3
                        if len(distribution["relative_positions"][key]["examples"]) < 3:
                            example = {
                                "vk_id": vk_id,
                                "phone": phone,
                                "vk_record_index": vk_idx,
                                "phone_record_index": phone_idx,
                                "database": db_name
                            }
                            distribution["relative_positions"][key]["examples"].append(example)

    # Сортируем шаблоны по количеству вхождений
    distribution["phone_patterns"].sort(key=lambda x: x["count"], reverse=True)

    # Определяем наиболее вероятный шаблон расположения
    if distribution["relative_positions"]:
        most_common_pos = max(distribution["relative_positions"].items(),
                              key=lambda x: x[1]["count"])

        distribution["most_likely_pattern"] = {
            "relative_position": most_common_pos[0],
            "count": most_common_pos[1]["count"],
            "examples": most_common_pos[1]["examples"]
        }

    return distribution


def suggest_extractor_function(analysis):
    """
    Предлагает реализацию функции-экстрактора на основе анализа
    """
    suggestion = "# Предлагаемая функция извлечения телефонов:\n\n"

    # Определяем наиболее вероятный шаблон
    phone_distribution = analysis.get("phone_distribution", {})
    most_likely = phone_distribution.get("most_likely_pattern", None)

    if most_likely:
        rel_pos = most_likely.get("relative_position")
        if rel_pos is not None:
            rel_pos = int(rel_pos)

            # Создаем шаблон функции в зависимости от обнаруженного шаблона
            if rel_pos == 0:  # Телефон в той же записи, что и VK ID
                suggestion += """
def extract_phones_specialized(response: dict, vk_ids: List[str]) -> Dict[str, List[str]]:
    \"\"\"
    Извлекает телефоны из ответа API. 
    Телефон находится в той же записи, что и VK ID.
    \"\"\"
    result = {vk_id: [] for vk_id in vk_ids}

    if "List" not in response:
        return result

    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        for record in data:
            if not isinstance(record, dict):
                continue

            # Ищем VK ID в записи
            found_vk_id = None
            for field_name, field_value in record.items():
                if any(id_key in field_name.lower() for id_key in ["vkid", "vk_id", "id"]):
                    str_value = str(field_value)
                    if str_value in vk_ids:
                        found_vk_id = str_value
                        break

            # Если нашли VK ID, ищем телефон в этой же записи
            if found_vk_id:
                for field_name, field_value in record.items():
                    if any(phone_key in field_name.lower() for phone_key in ["phone", "телефон", "тел"]):
                        if field_value:
                            digits = ''.join(c for c in str(field_value) if c.isdigit())
                            if digits.startswith('79') and len(digits) == 11:
                                if digits not in result[found_vk_id]:
                                    result[found_vk_id].append(digits)

    return result
"""
            elif rel_pos == 1:  # Телефон в следующей записи
                suggestion += """
def extract_phones_specialized(response: dict, vk_ids: List[str]) -> Dict[str, List[str]]:
    \"\"\"
    Извлекает телефоны из ответа API.
    Телефон находится в записи, следующей за записью с VK ID.
    \"\"\"
    result = {vk_id: [] for vk_id in vk_ids}

    if "List" not in response:
        return result

    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        for i, record in enumerate(data):
            if not isinstance(record, dict) or i + 1 >= len(data):
                continue

            # Ищем VK ID в текущей записи
            found_vk_id = None
            for field_name, field_value in record.items():
                if any(id_key in field_name.lower() for id_key in ["vkid", "vk_id", "id"]):
                    str_value = str(field_value)
                    if str_value in vk_ids:
                        found_vk_id = str_value
                        break

            # Если нашли VK ID, ищем телефон в следующей записи
            if found_vk_id:
                next_record = data[i + 1]
                if isinstance(next_record, dict):
                    for field_name, field_value in next_record.items():
                        if any(phone_key in field_name.lower() for phone_key in ["phone", "телефон", "тел"]):
                            if field_value:
                                digits = ''.join(c for c in str(field_value) if c.isdigit())
                                if digits.startswith('79') and len(digits) == 11:
                                    if digits not in result[found_vk_id]:
                                        result[found_vk_id].append(digits)

    return result
"""
            elif rel_pos == -1:  # Телефон в предыдущей записи
                suggestion += """
def extract_phones_specialized(response: dict, vk_ids: List[str]) -> Dict[str, List[str]]:
    \"\"\"
    Извлекает телефоны из ответа API.
    Телефон находится в записи, предшествующей записи с VK ID.
    \"\"\"
    result = {vk_id: [] for vk_id in vk_ids}

    if "List" not in response:
        return result

    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        for i, record in enumerate(data):
            if not isinstance(record, dict) or i <= 0:
                continue

            # Ищем VK ID в текущей записи
            found_vk_id = None
            for field_name, field_value in record.items():
                if any(id_key in field_name.lower() for id_key in ["vkid", "vk_id", "id"]):
                    str_value = str(field_value)
                    if str_value in vk_ids:
                        found_vk_id = str_value
                        break

            # Если нашли VK ID, ищем телефон в предыдущей записи
            if found_vk_id:
                prev_record = data[i - 1]
                if isinstance(prev_record, dict):
                    for field_name, field_value in prev_record.items():
                        if any(phone_key in field_name.lower() for phone_key in ["phone", "телефон", "тел"]):
                            if field_value:
                                digits = ''.join(c for c in str(field_value) if c.isdigit())
                                if digits.startswith('79') and len(digits) == 11:
                                    if digits not in result[found_vk_id]:
                                        result[found_vk_id].append(digits)

    return result
"""
            else:  # Другое расположение
                suggestion += f"""
def extract_phones_specialized(response: dict, vk_ids: List[str]) -> Dict[str, List[str]]:
    \"\"\"
    Извлекает телефоны из ответа API.
    Телефон находится в записи на позиции {rel_pos} относительно записи с VK ID.
    \"\"\"
    result = {{vk_id: [] for vk_id in vk_ids}}

    if "List" not in response:
        return result

    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        for i, record in enumerate(data):
            if not isinstance(record, dict):
                continue

            # Определяем индекс записи с телефоном
            phone_idx = i + ({rel_pos})
            if phone_idx < 0 or phone_idx >= len(data):
                continue

            # Ищем VK ID в текущей записи
            found_vk_id = None
            for field_name, field_value in record.items():
                if any(id_key in field_name.lower() for id_key in ["vkid", "vk_id", "id"]):
                    str_value = str(field_value)
                    if str_value in vk_ids:
                        found_vk_id = str_value
                        break

            # Если нашли VK ID, ищем телефон в соответствующей записи
            if found_vk_id:
                phone_record = data[phone_idx]
                if isinstance(phone_record, dict):
                    for field_name, field_value in phone_record.items():
                        if any(phone_key in field_name.lower() for phone_key in ["phone", "телефон", "тел"]):
                            if field_value:
                                digits = ''.join(c for c in str(field_value) if c.isdigit())
                                if digits.startswith('79') and len(digits) == 11:
                                    if digits not in result[found_vk_id]:
                                        result[found_vk_id].append(digits)

    return result
"""
    else:
        # Если не удалось определить шаблон, предлагаем универсальную функцию
        suggestion += """
def extract_phones_universal(response: dict, vk_ids: List[str]) -> Dict[str, List[str]]:
    \"\"\"
    Универсальная функция извлечения телефонов, которая пытается найти
    телефоны в окрестности ±3 записей от записи с VK ID.
    \"\"\"
    result = {vk_id: [] for vk_id in vk_ids}

    if "List" not in response:
        return result

    for db_name, db_info in response["List"].items():
        if "Data" not in db_info or not isinstance(db_info["Data"], list):
            continue

        data = db_info["Data"]

        # Шаг 1: Находим все записи с VK ID
        vk_id_positions = {}
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                continue

            for field_name, field_value in record.items():
                for vk_id in vk_ids:
                    if str(field_value) == vk_id:
                        if vk_id not in vk_id_positions:
                            vk_id_positions[vk_id] = []
                        vk_id_positions[vk_id].append(i)

        # Шаг 2: Для каждого найденного VK ID ищем телефоны в окрестности
        for vk_id, positions in vk_id_positions.items():
            for pos in positions:
                # Проверяем окрестность ±3 записей
                for offset in range(-3, 4):
                    check_idx = pos + offset
                    if 0 <= check_idx < len(data):
                        check_record = data[check_idx]
                        if not isinstance(check_record, dict):
                            continue

                        for field_name, field_value in check_record.items():
                            if any(phone_key in field_name.lower() for phone_key in 
                                  ["phone", "телефон", "тел", "мобильный"]) and field_value:
                                digits = ''.join(c for c in str(field_value) if c.isdigit())
                                if digits.startswith('79') and len(digits) == 11:
                                    if digits not in result[vk_id]:
                                        result[vk_id].append(digits)

    return result
"""

    return suggestion


def main():
    """Основная функция"""
    parser = setup_argparser()
    args = parser.parse_args()

    # Определяем список VK ID
    vk_ids = []
    if args.file:
        vk_ids = load_vk_ids_from_file(args.file)
    else:
        vk_ids = args.ids  # Изменено с args.vk_ids на args.ids

    if not vk_ids:
        print("Ошибка: не указан ни один VK ID")
        parser.print_help()
        sys.exit(1)

    # Используем токен из аргументов или из конфигурации
    token = args.token or LEAKOSINT_API_TOKEN

    # Создаем директорию для анализа, если её нет
    analysis_dir = TEMP_DIR / "analysis" if not args.output else Path(args.output)
    os.makedirs(analysis_dir, exist_ok=True)

    # Формируем имя файла с временной меткой
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"api_response_analysis_{timestamp}.json"
    filepath = os.path.join(analysis_dir, filename)

    print(f"Анализатор ответов API для LeakOSINT")
    print(f"===============================")
    print(f"VK ID для анализа: {', '.join(vk_ids)}")
    print(f"API URL: {API_URL}")
    print(f"Язык результатов: {args.lang}")
    print(f"Лимит поиска: {args.limit}")
    print(f"===============================")

    try:
        # Создаем экземпляр API клиента
        api_client = APIClient(token=token, url=API_URL)

        print(f"Отправка пакетного запроса к API...")
        start_time = time.time()

        # Делаем запрос к API
        # Для теста используем строку с разделителями \n, так как массив не работает
        request_str = "\n".join(vk_ids)
        response = api_client.make_request(
            query=request_str,
            lang=args.lang,
            limit=args.limit
        )

        # Если ошибка, выводим её и выходим
        if "error" in response:
            print(f"❌ Ошибка при запросе к API: {response['error']}")
            sys.exit(1)

        execution_time = time.time() - start_time
        print(f"Запрос выполнен за {execution_time:.2f} секунд")

        # Создаем данные для анализа
        analysis_data = {
            "timestamp": timestamp,
            "vk_ids": vk_ids,
            "api_response": response,
            "analysis_hints": {
                "description": "Этот файл содержит ответ API на пакетный запрос для анализа",
                "requested_ids": vk_ids,
                "known_phone_patterns": [
                    "phone", "телефон", "тел", "мобильный", "mobile"
                ],
                "known_id_patterns": [
                    "vkid", "vk_id", "id", "userid", "user_id"
                ]
            }
        }

        # Добавляем автоматический анализ
        print(f"Выполнение предварительного анализа...")
        analysis_data["initial_analysis"] = perform_initial_analysis(response, vk_ids)

        # Предлагаем реализацию функции извлечения телефонов
        analysis_data["suggested_extractor"] = suggest_extractor_function(analysis_data["initial_analysis"])

        # Записываем в файл
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2)

        print(f"✅ Ответ API успешно сохранен в файл для анализа:")
        print(f"   {filepath}")

        # Выводим краткие результаты анализа
        print("\n📊 Краткие результаты анализа:")
        print("===============================")

        analysis = analysis_data["initial_analysis"]

        print(f"Базы данных в ответе: {len(analysis['databases'])}")

        # Выводим количество найденных записей для каждого ID
        print("\nНайденные вхождения VK ID:")
        for vk_id, count in analysis.get("id_occurrences", {}).items():
            print(f"  VK ID {vk_id}: {count} вхождений")

        # Выводим информацию о потенциальных полях с телефонами
        print("\nПотенциальные поля с телефонами:")
        for field in analysis.get("potential_phone_fields", []):
            print(f"  {field}")

        # Выводим информацию о расположении телефонов
        phone_distribution = analysis.get("phone_distribution", {})
        print(f"\nНайдено телефонов: {phone_distribution.get('phone_count', 0)}")

        if "most_likely_pattern" in phone_distribution:
            pattern = phone_distribution["most_likely_pattern"]
            print(f"\nНаиболее вероятный шаблон расположения телефонов:")
            print(f"  Относительная позиция: {pattern['relative_position']} (встречается {pattern['count']} раз)")

            if int(pattern['relative_position']) == 0:
                print("  Телефоны находятся в той же записи, что и VK ID")
            elif int(pattern['relative_position']) == 1:
                print("  Телефоны находятся в следующей записи после VK ID")
            elif int(pattern['relative_position']) == -1:
                print("  Телефоны находятся в предыдущей записи перед VK ID")
            else:
                print(f"  Телефоны находятся на позиции {pattern['relative_position']} относительно записи с VK ID")

        print("\n===============================")
        print(f"На основе анализа создан шаблон специализированной функции извлечения телефонов.")
        print(f"Вы можете найти его в файле {filepath} в секции 'suggested_extractor'.")

    except Exception as e:
        print(f"❌ Ошибка при анализе ответа API: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()