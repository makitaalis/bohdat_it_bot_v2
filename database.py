#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для работы с базой данных и кешированием результатов
"""
import os

import json
import time
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Union

from config import DB_PATH, CACHE_TTL, CACHE_ENABLED
from logger import logger, log_cache_hit, log_cache_miss


class Database:
    """Класс для работы с базой  данных SQLite с учетом многопоточности"""

    def __init__(self, db_path=DB_PATH):
        """
        Инициализация базы данных

        Args:
            db_path (str, optional): Путь к файлу базы данных
        """
        self.db_path = db_path
        self.connection = None
        self.lock = threading.RLock()  # Рекурсивный мьютекс для потокобезопасности

        # Создаем директорию для базы данных, если она не существует
        db_dir = os.path.dirname(str(db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # Создаем таблицы при инициализации
        try:
            with self.get_connection() as conn:
                self.create_tables(conn)
        except Exception as e:
            logger.error(f"Критическая ошибка инициализации базы данных: {e}")
            # Пытаемся восстановить базу, удалив поврежденный файл
            try:
                if os.path.exists(str(db_path)):
                    backup_path = f"{db_path}.bak"
                    os.rename(str(db_path), backup_path)
                    logger.warning(f"Создана резервная копия поврежденной базы: {backup_path}")
                with self.get_connection() as conn:
                    self.create_tables(conn)
                logger.info("База данных успешно восстановлена")
            except Exception as recovery_error:
                logger.critical(f"Не удалось восстановить базу данных: {recovery_error}")

    def get_connection(self):
        """
        Получение соединения с базой данных, адаптировано для многопоточности

        Returns:
            sqlite3.Connection: Соединение с базой данных
        """
        # Создаем новое соединение для каждого потока
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
            conn.row_factory = sqlite3.Row

            # Включаем внешние ключи
            conn.execute("PRAGMA foreign_keys = ON")

            # Включаем режим записи с ожиданием вместо ошибки
            conn.execute("PRAGMA busy_timeout = 5000")

            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def check_database_health(self):
        """
        Проверяет состояние базы данных и наличие всех необходимых таблиц

        Returns:
            bool: True, если база данных в порядке, иначе False
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Проверка наличия всех необходимых таблиц
                for table in ["users", "cache", "user_settings", "query_logs", "phone_numbers", "search_patterns"]:
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                    if not cursor.fetchone():
                        logger.warning(f"Таблица {table} не найдена в базе данных")
                        # Пересоздаем структуру базы данных
                        self.create_tables(conn)
                        return False
            return True
        except Exception as e:
            logger.error(f"Ошибка при проверке состояния базы данных: {e}")
            return False

    def create_tables(self, conn=None):
        """
        Создание необходимых таблиц в базе данных

        Args:
            conn (sqlite3.Connection, optional): Соединение с базой данных
        """
        with self.lock:  # Обеспечиваем потокобезопасность
            close_conn = False
            if conn is None:
                conn = self.get_connection()
                close_conn = True

            cursor = conn.cursor()

            # Транзакция для создания всех таблиц
            cursor.execute('BEGIN TRANSACTION')

            try:
                # Таблица пользователей
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    telegram_id INTEGER UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP,
                    request_count INTEGER DEFAULT 0
                )
                ''')

                # Таблица кеша
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    id INTEGER PRIMARY KEY,
                    query TEXT UNIQUE,
                    response TEXT,
                    created_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    hit_count INTEGER DEFAULT 0,
                    has_phone_numbers INTEGER DEFAULT 0
                )
                ''')

                # Таблица настроек пользователей
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_settings (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER UNIQUE,
                    language TEXT DEFAULT 'ru',
                    results_per_page INTEGER DEFAULT 5,
                    FOREIGN KEY (user_id) REFERENCES users(telegram_id)
                )
                ''')

                # Таблица логов запросов
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_logs (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    query TEXT,
                    api_called BOOLEAN,
                    cached BOOLEAN,
                    status_code INTEGER,
                    response_size INTEGER,
                    processing_time REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')

                # Таблица номеров телефонов
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS phone_numbers (
                    id INTEGER PRIMARY KEY,
                    phone TEXT,
                    vk_id TEXT,
                    full_name TEXT,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(phone, vk_id)
                )
                ''')

                # Таблица шаблонов поиска
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS search_patterns (
                    id INTEGER PRIMARY KEY,
                    original_query TEXT,
                    method TEXT,
                    source_name TEXT,
                    confidence REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')

                # Завершаем транзакцию
                cursor.execute('COMMIT')
                logger.info("Database tables created successfully")
            except Exception as e:
                # В случае ошибки откатываем транзакцию
                cursor.execute('ROLLBACK')
                logger.error(f"Error creating database tables: {e}")
                raise

            finally:
                if close_conn:
                    conn.close()

    def save_search_pattern(self, original_query, method, source_name, confidence):
        """
        Сохранение успешного паттерна поиска

        Args:
            original_query (str): Исходный запрос
            method (str): Метод поиска
            source_name (str): Название источника данных
            confidence (float): Уверенность

        Returns:
            bool: True, если сохранение успешно
        """
        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        '''
                        INSERT INTO search_patterns (original_query, method, source_name, confidence)
                        VALUES (?, ?, ?, ?)
                        ''',
                        (original_query, method, source_name, confidence)
                    )
                    conn.commit()
                    return True
            except sqlite3.Error as e:
                logger.error(f"Error saving search pattern: {e}")
                return False

    def save_user(self, telegram_id, username=None, first_name=None, last_name=None):
        """
        Сохранение информации о пользователе в базу данных

        Args:
            telegram_id (int): ID пользователя в Telegram
            username (str, optional): Имя пользователя в Telegram
            first_name (str, optional): Имя пользователя
            last_name (str, optional): Фамилия пользователя

        Returns:
            int: ID пользователя в базе данных
        """
        with self.lock:  # Потокобезопасный блок
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()

                    # Проверяем, существует ли пользователь
                    cursor.execute(
                        'SELECT id FROM users WHERE telegram_id = ?',
                        (telegram_id,)
                    )
                    user = cursor.fetchone()

                    now = datetime.now()

                    if user:
                        # Обновляем информацию и last_activity
                        cursor.execute(
                            '''
                            UPDATE users SET 
                                username = COALESCE(?, username),
                                first_name = COALESCE(?, first_name),
                                last_name = COALESCE(?, last_name),
                                last_activity = ?
                            WHERE telegram_id = ?
                            ''',
                            (username, first_name, last_name, now, telegram_id)
                        )
                        user_id = user['id']
                    else:
                        # Создаем нового пользователя
                        cursor.execute(
                            '''
                            INSERT INTO users (telegram_id, username, first_name, last_name, created_at, last_activity)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''',
                            (telegram_id, username, first_name, last_name, now, now)
                        )
                        user_id = cursor.lastrowid

                        # Создаем настройки по умолчанию для нового пользователя
                        cursor.execute(
                            'INSERT INTO user_settings (user_id) VALUES (?)',
                            (telegram_id,)
                        )

                    conn.commit()
                    return user_id
            except sqlite3.Error as e:
                logger.error(f"Database error when saving user: {e}")
                return None

    def update_user_activity(self, telegram_id):
        """
        Обновление времени последней активности пользователя

        Args:
            telegram_id (int): ID пользователя в Telegram
        """
        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    now = datetime.now()
                    cursor.execute(
                        'UPDATE users SET last_activity = ?, request_count = request_count + 1 WHERE telegram_id = ?',
                        (now, telegram_id)
                    )
                    conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Database error when updating user activity: {e}")

    def get_user_settings(self, telegram_id):
        """
        Получение настроек пользователя

        Args:
            telegram_id (int): ID пользователя в Telegram

        Returns:
            dict: Настройки пользователя
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Сначала проверяем/создаем пользователя
                cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
                user = cursor.fetchone()

                if not user:
                    # Создаем пользователя, если он не существует
                    now = datetime.now()
                    cursor.execute(
                        'INSERT INTO users (telegram_id, created_at, last_activity) VALUES (?, ?, ?)',
                        (telegram_id, now, now)
                    )
                    conn.commit()

                # Теперь пробуем получить настройки
                cursor.execute(
                    'SELECT * FROM user_settings WHERE user_id = ?',
                    (telegram_id,)
                )
                settings = cursor.fetchone()

                if not settings:
                    # Создаем настройки по умолчанию, если их нет
                    with self.lock:  # Потокобезопасный блок
                        cursor.execute(
                            'INSERT INTO user_settings (user_id) VALUES (?)',
                            (telegram_id,)
                        )
                        conn.commit()
                        cursor.execute(
                            'SELECT * FROM user_settings WHERE user_id = ?',
                            (telegram_id,)
                        )
                        settings = cursor.fetchone()

                return dict(settings) if settings else {}
        except sqlite3.Error as e:
            logger.error(f"Database error when getting user settings: {e}")
            return {"language": "ru", "results_per_page": 5}  # Возвращаем настройки по умолчанию

    def update_user_settings(self, telegram_id, settings):
        """
        Обновление настроек пользователя

        Args:
            telegram_id (int): ID пользователя в Telegram
            settings (dict): Новые настройки

        Returns:
            bool: True, если обновление успешно
        """
        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    # Создаем список пар ключ-значение и список значений для запроса
                    set_clause = ', '.join([f"{key} = ?" for key in settings.keys()])
                    values = list(settings.values())
                    values.append(telegram_id)

                    cursor.execute(
                        f'UPDATE user_settings SET {set_clause} WHERE user_id = ?',
                        values
                    )
                    conn.commit()
                    return True
            except sqlite3.Error as e:
                logger.error(f"Error updating user settings: {e}")
                return False

    def _check_for_phone_numbers(self, response):
        """
        Проверяет наличие телефонных номеров в ответе API

        Args:
            response (dict): Ответ API

        Returns:
            bool: True, если найдены телефонные номера, иначе False
        """
        if not isinstance(response, dict) or "List" not in response:
            return False

        for db_name, db_info in response["List"].items():
            if db_name == "No results found" or "Data" not in db_info:
                continue

            for record in db_info["Data"]:
                for field_name, field_value in record.items():
                    # Ищем поля, которые могут содержать телефонные номера
                    if isinstance(field_name, str) and "phone" in field_name.lower():
                        return True

                    # Ищем значения, похожие на телефонные номера
                    if isinstance(field_value, str) and any(c.isdigit() for c in field_value):
                        # Простая проверка: если строка содержит 7+ цифр подряд, это может быть телефон
                        digits = ''.join(c for c in field_value if c.isdigit())
                        if len(digits) >= 7:
                            return True

        return False

    def extract_and_save_phone_numbers(self, response, vk_id):
        """
        Извлекает и сохраняет телефонные номера из ответа API

        Args:
            response (dict): Ответ API
            vk_id (str): VK ID, для которого был сделан запрос

        Returns:
            int: Количество найденных телефонных номеров
        """
        if not isinstance(response, dict) or "List" not in response:
            return 0

        phone_count = 0

        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()

                    for db_name, db_info in response["List"].items():
                        if db_name == "No results found" or "Data" not in db_info:
                            continue

                        for record in db_info["Data"]:
                            full_name = None
                            phone = None

                            # Ищем имя
                            for name_field in ["FullName", "FirstName", "LastName"]:
                                if name_field in record:
                                    full_name = record[name_field]
                                    if "FirstName" in record and "LastName" in record and name_field != "FullName":
                                        full_name = f"{record['FirstName']} {record['LastName']}"
                                    break

                            # Ищем телефон
                            for field_name, field_value in record.items():
                                if isinstance(field_name, str) and "phone" in field_name.lower() and field_value:
                                    phone = str(field_value)

                                    # Нормализуем номер телефона (оставляем только цифры)
                                    phone = ''.join(c for c in phone if c.isdigit())

                                    # Если номер достаточно длинный, сохраняем его
                                    if len(phone) >= 7:
                                        try:
                                            cursor.execute(
                                                '''
                                                INSERT OR IGNORE INTO phone_numbers (phone, vk_id, full_name, source)
                                                VALUES (?, ?, ?, ?)
                                                ''',
                                                (phone, vk_id, full_name, db_name)
                                            )
                                            phone_count += 1
                                        except sqlite3.Error as e:
                                            logger.error(f"Error saving phone number: {e}")

                    conn.commit()
                return phone_count
            except sqlite3.Error as e:
                logger.error(f"Database error when extracting phone numbers: {e}")
                return 0

    def cache_response(self, query, response):
        """
        Кеширование ответа API

        Args:
            query (str): Запрос к API
            response (dict): Ответ API

        Returns:
            bool: True, если кеширование успешно
        """
        if not CACHE_ENABLED:
            return False

        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    now = datetime.now()
                    expires_at = datetime.fromtimestamp(time.time() + CACHE_TTL)

                    # Проверяем наличие телефонных номеров в ответе
                    has_phones = 1 if self._check_for_phone_numbers(response) else 0

                    # Преобразуем ответ в JSON строку
                    response_json = json.dumps(response, ensure_ascii=False)

                    # Добавляем или обновляем запись в кеше
                    cursor.execute(
                        '''
                        INSERT INTO cache (query, response, created_at, expires_at, has_phone_numbers)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(query) DO UPDATE SET
                            response = excluded.response,
                            created_at = excluded.created_at,
                            expires_at = excluded.expires_at,
                            has_phone_numbers = excluded.has_phone_numbers
                        ''',
                        (query, response_json, now, expires_at, has_phones)
                    )

                    conn.commit()
                    return True
            except sqlite3.Error as e:
                logger.error(f"Error caching response: {e}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error caching response: {e}")
                return False

    def get_cached_response(self, query):
        """
        Получение кешированного ответа API

        Args:
            query (str): Запрос к API

        Returns:
            dict: Кешированный ответ или None
        """
        if not CACHE_ENABLED:
            return None

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = datetime.now()

                cursor.execute(
                    '''
                    SELECT response, expires_at FROM cache
                    WHERE query = ? AND expires_at > ?
                    ''',
                    (query, now)
                )

                result = cursor.fetchone()

                if result:
                    # Обновляем счетчик попаданий в кеш
                    with self.lock:  # Обеспечиваем потокобезопасность
                        cursor.execute(
                            'UPDATE cache SET hit_count = hit_count + 1 WHERE query = ?',
                            (query,)
                        )
                        conn.commit()

                    log_cache_hit(query)

                    # Преобразуем JSON строку обратно в dict
                    try:
                        return json.loads(result['response'])
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding cached JSON: {e}")
                        return None

                log_cache_miss(query)
                return None
        except sqlite3.Error as e:
            logger.error(f"Database error when getting cached response: {e}")
            return None

    def search_phone_number(self, phone):
        """
        Поиск VK ID по номеру телефона

        Args:
            phone (str): Номер телефона для поиска

        Returns:
            list: Список найденных VK ID с именами
        """
        # Проверяем параметр
        if not phone:
            return []

        # Нормализуем номер телефона (оставляем только цифры)
        normalized_phone = ''.join(c for c in phone if c.isdigit())

        # Проверяем длину
        if len(normalized_phone) < 7:
            return []

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Поиск по полному номеру
                cursor.execute(
                    '''
                    SELECT vk_id, full_name, source FROM phone_numbers
                    WHERE phone = ?
                    ''',
                    (normalized_phone,)
                )

                results = cursor.fetchall()

                # Если ничего не найдено, ищем по частичному совпадению
                if not results:
                    cursor.execute(
                        '''
                        SELECT vk_id, full_name, source FROM phone_numbers
                        WHERE phone LIKE ?
                        ''',
                        (f"%{normalized_phone}%",)
                    )

                    results = cursor.fetchall()

                return [dict(row) for row in results]
        except sqlite3.Error as e:
            logger.error(f"Database error when searching phone number: {e}")
            return []

    def log_query(self, user_id, query, api_called, cached, status_code, response_size, processing_time):
        """
        Логирование информации о запросе

        Args:
            user_id (int): ID пользователя
            query (str): Запрос к API
            api_called (bool): Был ли вызван API
            cached (bool): Был ли ответ получен из кеша
            status_code (int): Код ответа
            response_size (int): Размер ответа в байтах
            processing_time (float): Время обработки запроса в секундах
        """
        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        '''
                        INSERT INTO query_logs (
                            user_id, query, api_called, cached, status_code, 
                            response_size, processing_time
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''',
                        (user_id, query, api_called, cached, status_code, response_size, processing_time)
                    )
                    conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Database error when logging query: {e}")

    def clean_expired_cache(self):
        """
        Удаление устаревших записей из кеша

        Returns:
            int: Количество удаленных записей
        """
        with self.lock:  # Обеспечиваем потокобезопасность
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    now = datetime.now()

                    cursor.execute(
                        'DELETE FROM cache WHERE expires_at < ?',
                        (now,)
                    )

                    deleted_count = cursor.rowcount
                    conn.commit()

                    if deleted_count > 0:
                        logger.info(f"Cleaned {deleted_count} expired cache entries")

                    return deleted_count
            except sqlite3.Error as e:
                logger.error(f"Database error when cleaning expired cache: {e}")
                return 0

    def get_cache_stats(self):
        """
        Получение статистики кеша

        Returns:
            dict: Статистика кеша
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Общее количество записей
                cursor.execute('SELECT COUNT(*) as total FROM cache')
                total = cursor.fetchone()['total']

                # Количество записей с телефонными номерами
                cursor.execute('SELECT COUNT(*) as phone_count FROM cache WHERE has_phone_numbers = 1')
                phone_count = cursor.fetchone()['phone_count']

                # Средний размер ответа
                cursor.execute('SELECT AVG(LENGTH(response)) as avg_size FROM cache')
                avg_size = cursor.fetchone()['avg_size']

                # Самые популярные запросы
                cursor.execute(
                    'SELECT query, hit_count FROM cache ORDER BY hit_count DESC LIMIT 5'
                )
                popular_queries = [dict(row) for row in cursor.fetchall()]

                # Статистика по телефонным номерам
                cursor.execute('SELECT COUNT(*) as total FROM phone_numbers')
                total_phones = cursor.fetchone()['total']

                # Количество уникальных VK ID с телефонами
                cursor.execute('SELECT COUNT(DISTINCT vk_id) as unique_vk_ids FROM phone_numbers')
                unique_vk_ids = cursor.fetchone()['unique_vk_ids']

                return {
                    'total_entries': total,
                    'phone_entries': phone_count,
                    'avg_response_size': avg_size,
                    'popular_queries': popular_queries,
                    'total_phones': total_phones,
                    'unique_vk_ids': unique_vk_ids
                }
        except sqlite3.Error as e:
            logger.error(f"Database error when getting cache stats: {e}")
            return {
                'total_entries': 0,
                'phone_entries': 0,
                'avg_response_size': 0,
                'popular_queries': [],
                'total_phones': 0,
                'unique_vk_ids': 0
            }
        except Exception as e:
            logger.error(f"Unexpected error getting cache stats: {e}")
            return {
                'total_entries': 0,
                'phone_entries': 0,
                'avg_response_size': 0,
                'popular_queries': [],
                'total_phones': 0,
                'unique_vk_ids': 0
            }

    def delete_cached_response(self, query):
        """
        Удаление записи из кеша по ключу

        Args:
            query (str): Ключ кеша

        Returns:
            bool: True, если удаление успешно
        """
        with self.lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('DELETE FROM cache WHERE query = ?', (query,))
                    conn.commit()
                    return cursor.rowcount > 0
            except sqlite3.Error as e:
                logger.error(f"Ошибка при удалении записи из кеша: {e}")
                return False
# Создаем экземпляр базы данных
db = Database()