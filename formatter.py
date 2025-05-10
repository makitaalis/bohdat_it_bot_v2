#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для форматирования результатов API в HTML и другие форматы
"""

import json
import html
import os
import re
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
from datetime import datetime, timedelta

from config import TEMP_DIR, JSON_DIR
from logger import logger


class ResponseFormatter:
    """Класс для форматирования ответов API"""

    @staticmethod
    def format_html(response: Dict[str, Any], vk_id: str) -> str:
        """
        Форматирование ответа API в HTML

        Args:
            response (Dict[str, Any]): Ответ API
            vk_id (str): VK ID для которого был сделан запрос

        Returns:
            str: Отформатированный HTML
        """
        if "error" in response:
            return ResponseFormatter._html_error_template(response["error"])

        # Проверяем наличие результатов
        if "List" not in response or not response["List"]:
            return ResponseFormatter._html_no_results_template(vk_id)

        html_content = []

        # Добавляем заголовок
        html_content.append(f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Результаты поиска по VK ID: {html.escape(vk_id)}</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    margin: 0;
                    padding: 20px;
                    background-color: #f4f4f9;
                    color: #333;
                }}
                .container {{
                    max-width: 1000px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                }}
                h1, h2, h3 {{
                    color: #333;
                }}
                .database {{
                    margin-bottom: 30px;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    padding: 15px;
                    background-color: #f8f8f8;
                }}
                .database h2 {{
                    margin-top: 0;
                    border-bottom: 1px solid #ddd;
                    padding-bottom: 10px;
                }}
                .database-with-phones {{
                    border-left: 5px solid #4CAF50;
                }}
                .record {{
                    margin-bottom: 20px;
                    padding: 10px;
                    background-color: #fff;
                    border: 1px solid #eee;
                    border-radius: 4px;
                }}
                .field {{
                    margin-bottom: 5px;
                }}
                .field-name {{
                    font-weight: bold;
                }}
                .phone-field {{
                    background-color: #e8f5e9;
                    padding: 3px 8px;
                    border-radius: 3px;
                    font-weight: bold;
                }}
                .timestamp {{
                    font-size: 0.8em;
                    color: #666;
                    text-align: center;
                    margin-top: 20px;
                }}
                .summary {{
                    background-color: #e3f2fd;
                    padding: 15px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }}
                .summary h3 {{
                    margin-top: 0;
                }}
                .phone-summary {{
                    margin-top: 10px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Результаты поиска по VK ID: {html.escape(vk_id)}</h1>
        """)

        # Добавляем сводку о найденных данных
        total_records = 0
        total_phones = 0
        databases_with_phones = []

        # Подсчет записей и телефонов
        for db_name, db_info in response["List"].items():
            if db_name == "No results found" or "Data" not in db_info or not db_info["Data"]:
                continue

            db_records = len(db_info["Data"])
            total_records += db_records

            # Подсчет телефонов в этой базе
            db_phones = 0
            for record in db_info["Data"]:
                for field_name, field_value in record.items():
                    if "phone" in field_name.lower() and field_value and field_value != "NULL":
                        db_phones += 1
                        total_phones += 1

            if db_phones > 0:
                databases_with_phones.append((db_name, db_phones))

        # Добавляем сводку
        if total_records > 0:
            html_content.append('<div class="summary">')
            html_content.append(f'<h3>Сводка по результатам поиска</h3>')
            html_content.append(f'<p>Найдено {total_records} записей в {len(response["List"])} базах данных.</p>')

            if total_phones > 0:
                html_content.append('<div class="phone-summary">')
                html_content.append(f'<p><strong>Обнаружено {total_phones} телефонных номеров:</strong></p>')
                html_content.append('<ul>')
                for db_name, phone_count in databases_with_phones:
                    html_content.append(f'<li>{html.escape(db_name)}: {phone_count} номеров</li>')
                html_content.append('</ul>')
                html_content.append('</div>')

            html_content.append('</div>')

        # Обрабатываем результаты по каждой базе данных
        for db_name, db_info in response["List"].items():
            # Проверяем, есть ли телефоны в этой базе
            has_phones = False
            for record in db_info.get("Data", []):
                for field_name in record.keys():
                    if "phone" in field_name.lower():
                        has_phones = True
                        break
                if has_phones:
                    break

            css_class = "database database-with-phones" if has_phones else "database"
            html_content.append(f'<div class="{css_class}">')
            html_content.append(f'<h2>{html.escape(db_name)}</h2>')

            # Добавляем информацию о базе данных (InfoLeak)
            if "InfoLeak" in db_info:
                html_content.append(f'<p>{html.escape(db_info["InfoLeak"])}</p>')

            # Если нет данных или это сообщение "No results found"
            if db_name == "No results found" or "Data" not in db_info or not db_info["Data"]:
                html_content.append('<p>Данные не найдены</p>')
                html_content.append('</div>')
                continue

            # Обрабатываем записи
            for record in db_info["Data"]:
                html_content.append('<div class="record">')
                for field_name, field_value in record.items():
                    safe_value = html.escape(str(field_value)) if field_value is not None else ""

                    # Выделяем телефонные номера
                    field_class = "field"
                    if "phone" in field_name.lower() and field_value and field_value != "NULL":
                        # Форматируем телефон
                        formatted_phone = ResponseFormatter._format_phone_number(str(field_value))
                        safe_value = html.escape(formatted_phone)
                        field_class = "field phone-field"

                    html_content.append(
                        f'<div class="{field_class}"><span class="field-name">{html.escape(field_name)}:</span> {safe_value}</div>')
                html_content.append('</div>')

            html_content.append('</div>')

        # Добавляем временную метку и завершаем HTML
        now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        html_content.append(f'<div class="timestamp">Отчет сгенерирован: {now}</div>')
        html_content.append('</div></body></html>')

        return ''.join(html_content)

    @staticmethod
    def _html_error_template(error_message: str) -> str:
        """
        Шаблон HTML для ошибки

        Args:
            error_message (str): Сообщение об ошибке

        Returns:
            str: HTML-страница с ошибкой
        """
        return f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка при выполнении запроса</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    margin: 0;
                    padding: 20px;
                    background-color: #f4f4f9;
                    color: #333;
                }}
                .container {{
                    max-width: 800px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                    text-align: center;
                }}
                h1 {{
                    color: #e74c3c;
                }}
                .error-message {{
                    font-size: 1.2em;
                    margin: 20px 0;
                    padding: 10px;
                    background-color: #ffeaea;
                    border-radius: 5px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Ошибка при выполнении запроса</h1>
                <div class="error-message">{html.escape(error_message)}</div>
                <p>Пожалуйста, проверьте ваш запрос и попробуйте снова.</p>
            </div>
        </body>
        </html>
        """

    @staticmethod
    def _html_no_results_template(vk_id: str) -> str:
        """
        Шаблон HTML для случая, когда результаты не найдены

        Args:
            vk_id (str): VK ID для которого был сделан запрос

        Returns:
            str: HTML-страница с сообщением об отсутствии результатов
        """
        return f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Результаты не найдены</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    margin: 0;
                    padding: 20px;
                    background-color: #f4f4f9;
                    color: #333;
                }}
                .container {{
                    max-width: 800px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
                    text-align: center;
                }}
                h1 {{
                    color: #3498db;
                }}
                .info-message {{
                    font-size: 1.2em;
                    margin: 20px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Результаты не найдены</h1>
                <div class="info-message">
                    По VK ID: <strong>{html.escape(vk_id)}</strong> не найдено никакой информации.
                </div>
                <p>Попробуйте другой идентификатор или измените параметры поиска.</p>
            </div>
        </body>
        </html>
        """

    @staticmethod
    def _format_phone_number(phone: str) -> str:
        """
        Форматирование номера телефона для лучшей читаемости

        Args:
            phone (str): Строка с номером телефона

        Returns:
            str: Отформатированный номер телефона
        """
        # Оставляем только цифры
        digits = ''.join(c for c in phone if c.isdigit())

        # Если номер слишком короткий, возвращаем как есть
        if len(digits) < 7:
            return phone

        # Российский формат для номеров начинающихся с 7 или 8 и содержащих 11 цифр
        if len(digits) == 11 and digits[0] in ('7', '8'):
            return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"

        # Простой формат для других номеров, группировка по 3-4 цифры
        parts = []
        i = 0
        while i < len(digits):
            if i == 0 and len(digits) > 10:
                # Код страны может быть 1-3 цифры
                cc_len = min(3, len(digits) - 10)
                parts.append("+" + digits[:cc_len])
                i += cc_len
            elif i == 0 or i == cc_len if 'cc_len' in locals() else False:
                # Код региона (обычно 3 цифры)
                region_len = min(3, len(digits) - i)
                parts.append("(" + digits[i:i + region_len] + ")")
                i += region_len
            elif len(digits) - i <= 4:
                # Последние 4 цифры
                parts.append(digits[i:])
                break
            else:
                # Группа по 3 цифры
                parts.append(digits[i:i + 3])
                i += 3

        return " ".join(parts)

    @staticmethod
    def format_telegram_message(response: Dict[str, Any], vk_id: str) -> str:
        """
        Форматирование ответа API для сообщения в Telegram с использованием HTML-форматирования

        Args:
            response (Dict[str, Any]): Ответ API
            vk_id (str): VK ID для которого был сделан запрос

        Returns:
            str: Отформатированный текст для Telegram с HTML-тегами
        """
        if "error" in response:
            return f"❌ <b>Ошибка при выполнении запроса:</b>\n\n{html.escape(response['error'])}"

        # Проверяем наличие результатов
        if "List" not in response or not response["List"]:
            return f"📭 <b>Результаты не найдены</b>\n\nПо VK ID: <code>{html.escape(vk_id)}</code> не найдено никакой информации."

        # Формируем текст сообщения
        message_parts = []
        message_parts.append(f"🔍 <b>Результаты поиска по VK ID: {html.escape(vk_id)}</b>\n")

        # Подсчет общего количества баз и записей с телефонами
        total_dbs = len(response["List"])
        dbs_with_phones = 0
        total_phone_records = 0

        for db_name, db_info in response["List"].items():
            if db_name == "No results found" or "Data" not in db_info or not db_info["Data"]:
                continue

            # Проверяем наличие телефонов в этой базе
            has_phones = False
            phone_count = 0

            for record in db_info["Data"]:
                for field_name, field_value in record.items():
                    if "phone" in field_name.lower() and field_value and str(field_value).strip():
                        has_phones = True
                        phone_count += 1

            if has_phones:
                dbs_with_phones += 1
                total_phone_records += phone_count

        # Добавляем сводку по найденным данным
        if total_dbs > 0:
            message_parts.append(f"📊 <b>Сводка:</b> Найдено данных в {total_dbs} базах")
            if dbs_with_phones > 0:
                message_parts.append(
                    f"📱 <b>Телефонные номера:</b> Найдено {total_phone_records} номеров в {dbs_with_phones} базах\n")

        # Ограничим количество баз данных для отображения в сообщении Telegram
        max_databases = 3
        db_count = 0

        # Сначала показываем базы с телефонами
        for db_name, db_info in response["List"].items():
            if db_count >= max_databases:
                break

            if db_name == "No results found" or "Data" not in db_info or not db_info["Data"]:
                continue

            # Проверяем, есть ли телефоны в этой базе
            has_phones = False
            for record in db_info["Data"]:
                for field_name in record.keys():
                    if "phone" in field_name.lower():
                        has_phones = True
                        break
                if has_phones:
                    break

            # Пропускаем базы без телефонов на первом проходе
            if not has_phones:
                continue

            message_parts.append(f"\n📁 <b>{html.escape(db_name)}</b> 📱")

            # Добавляем информацию о базе данных (InfoLeak)
            if "InfoLeak" in db_info:
                message_parts.append(f"<i>{html.escape(db_info['InfoLeak'])}</i>\n")

            # Ограничим количество записей для отображения
            max_records = 2
            record_count = 0

            # Обрабатываем записи
            for record in db_info["Data"]:
                # Ищем телефоны в этой записи
                contains_phone = False
                for field_name in record.keys():
                    if "phone" in field_name.lower():
                        contains_phone = True
                        break

                # Пропускаем записи без телефонов
                if not contains_phone:
                    continue

                if record_count >= max_records:
                    message_parts.append("<i>...и еще записи с телефонами</i>")
                    break

                message_parts.append("<pre>")
                for field_name, field_value in record.items():
                    value = str(field_value) if field_value is not None else ""
                    value = html.escape(value)

                    # Форматируем телефонные номера
                    if "phone" in field_name.lower() and value:
                        value = ResponseFormatter._format_phone_number(value)
                        field_name = f"📱 {field_name}"

                    # Экранируем имя поля
                    field_name = html.escape(field_name)

                    # Ограничиваем длину значения
                    if len(value) > 50:
                        value = value[:47] + "..."
                    message_parts.append(f"{field_name}: {value}")
                message_parts.append("</pre>")

                record_count += 1

            db_count += 1

        # Теперь показываем остальные базы без телефонов, если еще есть место
        if db_count < max_databases:
            for db_name, db_info in response["List"].items():
                if db_count >= max_databases:
                    break

                if db_name == "No results found" or "Data" not in db_info or not db_info["Data"]:
                    continue

                # Проверяем, есть ли телефоны в этой базе
                has_phones = False
                for record in db_info["Data"]:
                    for field_name in record.keys():
                        if "phone" in field_name.lower():
                            has_phones = True
                            break
                    if has_phones:
                        break

                # Пропускаем базы, которые уже отображали
                if has_phones:
                    continue

                message_parts.append(f"\n📁 <b>{html.escape(db_name)}</b>")

                # Добавляем информацию о базе данных (InfoLeak)
                if "InfoLeak" in db_info:
                    message_parts.append(f"<i>{html.escape(db_info['InfoLeak'])}</i>\n")

                # Ограничим количество записей для отображения
                max_records = 2
                record_count = 0

                # Обрабатываем записи
                for record in db_info["Data"]:
                    if record_count >= max_records:
                        message_parts.append("<i>...и еще записи</i>")
                        break

                    message_parts.append("<pre>")
                    for field_name, field_value in record.items():
                        value = str(field_value) if field_value is not None else ""
                        value = html.escape(value)
                        field_name = html.escape(field_name)
                        # Ограничиваем длину значения
                        if len(value) > 50:
                            value = value[:47] + "..."
                        message_parts.append(f"{field_name}: {value}")
                    message_parts.append("</pre>")

                    record_count += 1

                db_count += 1

        if db_count >= max_databases and len(response["List"]) > max_databases:
            message_parts.append(
                f"\n⚠️ <i>Есть еще {len(response['List']) - db_count} баз данных. Загрузите HTML файл для просмотра всех данных.</i>")

        # Добавляем временную метку
        now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        message_parts.append(f"\n⏱ <i>Отчет сгенерирован: {now}</i>")

        full_message = "\n".join(message_parts)

        # Telegram имеет ограничение на длину сообщения
        if len(full_message) > 4000:
            return full_message[:3997] + "..."

        return full_message

    @staticmethod
    def save_html_file(html_content: str, vk_id: str):
        """
        Сохранение HTML отчета в файл

        Args:
            html_content (str): HTML содержимое
            vk_id (str): VK ID для которого был сделан запрос

        Returns:
            Path: Путь к созданному файлу
        """
        # Создаем директорию, если её нет
        TEMP_DIR.mkdir(exist_ok=True)

        # Генерируем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"vk_id_{vk_id}_{timestamp}.html"
        file_path = TEMP_DIR / filename

        try:
            # Сохраняем HTML в файл
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            logger.info(f"HTML file saved: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error saving HTML file: {e}")
            return None

    @staticmethod
    def save_json_file(json_data: Dict[str, Any], vk_id: str):
        """
        Сохранение JSON ответа в файл

        Args:
            json_data (Dict[str, Any]): Данные JSON
            vk_id (str): VK ID для которого был сделан запрос

        Returns:
            Path: Путь к созданному файлу
        """
        # Создаем директорию, если её нет
        JSON_DIR.mkdir(exist_ok=True)

        # Генерируем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"vk_id_{vk_id}_{timestamp}.json"
        file_path = JSON_DIR / filename

        try:
            # Сохраняем JSON в файл
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)

            logger.info(f"JSON file saved: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error saving JSON file: {e}")
            return None

    @staticmethod
    def cleanup_old_files():
        """
        Очистка старых временных файлов (HTML и XLSX)

        Returns:
            int: Количество удаленных файлов
        """
        # Проверяем существование директории
        if not TEMP_DIR.exists():
            TEMP_DIR.mkdir(exist_ok=True)
            return 0

        # Срок хранения файлов - 2 дня
        retention_days = 2
        retention_time = datetime.now() - timedelta(days=retention_days)

        count = 0
        # Удаляем старые HTML и XLSX файлы
        for file_pattern in ["*.html", "*.xlsx"]:
            for file_path in TEMP_DIR.glob(file_pattern):
                try:
                    # Получаем время модификации файла
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)

                    # Если файл старше указанного срока, удаляем его
                    if file_time < retention_time:
                        file_path.unlink()
                        count += 1
                except PermissionError as e:
                    logger.error(f"Permission error deleting file {file_path}: {e}")
                except FileNotFoundError:
                    # Файл уже удален или не существует
                    pass
                except Exception as e:
                    logger.error(f"Error deleting file {file_path}: {e}")

        logger.info(f"Cleaned {count} old files from {TEMP_DIR}")
        return count

    @staticmethod
    def cleanup_old_json_files():
        """
        Очистка старых JSON файлов

        Returns:
            int: Количество удаленных файлов
        """
        # Проверяем существование директории
        if not JSON_DIR.exists():
            JSON_DIR.mkdir(exist_ok=True)
            return 0

        # Срок хранения файлов - 7 дней
        retention_days = 7
        retention_time = datetime.now() - timedelta(days=retention_days)

        count = 0
        for file_path in JSON_DIR.glob("*.json"):
            try:
                # Получаем время модификации файла
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)

                # Если файл старше указанного срока, удаляем его
                if file_time < retention_time:
                    file_path.unlink()
                    count += 1
            except PermissionError as e:
                logger.error(f"Permission error deleting JSON file {file_path}: {e}")
            except FileNotFoundError:
                # Файл уже удален или не существует
                pass
            except Exception as e:
                logger.error(f"Error deleting JSON file {file_path}: {e}")

        logger.info(f"Cleaned {count} old JSON files from {JSON_DIR}")
        return count


# Создаем экземпляр форматтера
formatter = ResponseFormatter()