#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль логирования для Telegram бота LeakOSINT
"""

import logging
import logging.handlers
import sys
import os
from pathlib import Path

from config import LOG_LEVEL, LOG_FORMAT, LOG_FILE


def setup_logger(name="LeakOSINTBot"):
    """
    Настройка логгера с ротацией файлов

    Args:
        name (str): Имя логгера

    Returns:
        logging.Logger: Настроенный логгер
    """
    # Преобразуем строковое представление уровня логирования в число
    numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"WARNING: Invalid log level: {LOG_LEVEL}. Using INFO.")
        numeric_level = logging.INFO

    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    # Очищаем существующие обработчики, если они есть
    if logger.handlers:
        logger.handlers.clear()

    try:
        # Проверяем, существует ли директория для логов
        log_path = Path(LOG_FILE)
        log_path.parent.mkdir(exist_ok=True)

        # Настраиваем формат сообщений
        formatter = logging.Formatter(LOG_FORMAT)

        # Обработчик для записи в файл с ротацией (каждый день, хранить 30 дней)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            LOG_FILE, when="midnight", interval=1, backupCount=30, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"WARNING: Could not set up file logging: {e}")

    # Обработчик для вывода в консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Информация о старте логгера
    logger.info(f"Logger {name} started with level {LOG_LEVEL}")

    return logger


# Создаём экземпляр логгера
logger = setup_logger()


def log_api_request(query, params):
    """
    Логирование запроса к API

    Args:
        query (str): Запрос к API
        params (dict): Параметры запроса
    """
    # Маскируем токен для безопасности
    if "token" in params:
        masked_token = params["token"][:8] + "..." if params["token"] else ""
        safe_params = params.copy()
        safe_params["token"] = masked_token
    else:
        safe_params = params

    logger.info(f"API Request: {query}, Params: {safe_params}")


def log_api_response(query, status_code, response_size):
    """
    Логирование ответа от API

    Args:
        query (str): Запрос к API
        status_code (int): Код ответа
        response_size (int): Размер ответа в байтах
    """
    logger.info(f"API Response: {query}, Status: {status_code}, Size: {response_size} bytes")


def log_cache_hit(query):
    """
    Логирование попадания в кеш

    Args:
        query (str): Запрос к API
    """
    logger.info(f"Cache hit for query: {query}")


def log_cache_miss(query):
    """
    Логирование промаха в кеше

    Args:
        query (str): Запрос к API
    """
    logger.info(f"Cache miss for query: {query}")


def log_user_action(user_id, username, action, query=None):
    """
    Логирование действий пользователя

    Args:
        user_id (int): ID пользователя
        username (str): Имя пользователя
        action (str): Действие
        query (str, optional): Запрос пользователя
    """
    # Защита от None
    username = username or "unknown"

    if query:
        logger.info(f"User {user_id} (@{username}) {action}: {query}")
    else:
        logger.info(f"User {user_id} (@{username}) {action}")


def log_error(error, context=None):
    """
    Логирование ошибок

    Args:
        error (Exception): Объект ошибки
        context (dict, optional): Контекст ошибки
    """
    if context:
        logger.error(f"Error: {error}, Context: {context}")
    else:
        logger.error(f"Error: {error}")

    # Логируем трассировку стека для более детальной отладки
    import traceback
    logger.debug(f"Error traceback: {traceback.format_exc()}")