#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Конфигурационный файл для Telegram бота LeakOSINT
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения  из .env файла, если он существует
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

# Токены и ключи
# Приоритет: 1. Переменные окружения 2. .env файл 3. Значения по умолчанию (только для разработки)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8148730915:AAGDGAJIq-eMLMoCpPYU0zgPesVTlANJ8hs")
LEAKOSINT_API_TOKEN = os.getenv("LEAKOSINT_API_TOKEN", "5505987961:FOMLSZaT")

# Параметры API
API_URL = os.getenv("API_URL", "https://leakosintapi.com/")
API_DEFAULT_LANG = os.getenv("API_DEFAULT_LANG", "ru")  # Язык результатов по умолчанию
API_DEFAULT_LIMIT = int(os.getenv("API_DEFAULT_LIMIT", "300"))  # Лимит поиска по умолчанию
API_DEFAULT_TYPE = os.getenv("API_DEFAULT_TYPE", "json")  # Тип ответа по умолчанию

# Параметры базы данных
DB_TYPE = os.getenv("DB_TYPE", "sqlite")  # sqlite, mysql, postgresql
DB_NAME = os.getenv("DB_NAME", "leakosint_cache.db")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")

# Пути к директориям и файлам
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
TEMP_DIR = BASE_DIR / "temp"
JSON_DIR = BASE_DIR / "json"  # Директория для JSON файлов

# SQLite файл находится в корне проекта
DB_PATH = BASE_DIR / DB_NAME

# Параметры кеширования
CACHE_TTL = int(os.getenv("CACHE_TTL", str(30 * 24 * 60 * 60)))  # Время жизни кеша в секундах (30 дней)
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "True").lower() in ("true", "1", "t", "yes", "y")  # Включить/выключить кеширование

# Параметры логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_FILE = LOG_DIR / "bot.log"

# Создаем необходимые директории, если они не существуют
LOG_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
JSON_DIR.mkdir(exist_ok=True)

# Настройки бота
BOT_ADMINS = [
    # Список ID администраторов бота
    int(admin_id) for admin_id in os.getenv("BOT_ADMINS", "").split(",") if admin_id.strip().isdigit()
]

BOT_MESSAGES = {
    "welcome": "Привет! Я бот для поиска информации по VK ID. Чтобы начать, используйте команду /vk [ID]",
    "help": """
Доступные команды:
/start - Начать работу с ботом
/help - Показать справку
/vk [ID] - Поиск информации по VK ID
/settings - Настройки
/status - Показать статус API
    """,
    "error": "Произошла ошибка: {error}",
    "no_results": "По вашему запросу ничего не найдено",
    "processing": "Обрабатываю запрос, пожалуйста, подождите...",
    "cache_hit": "Данные получены из кеша",
    "api_call": "Отправляю запрос к API...",
    "result_ready": "Результаты готовы. Вы можете скачать их или просмотреть прямо здесь.",
    "settings_updated": "Настройки обновлены",
}

# Настройки пользовательского интерфейса
UI_SETTINGS = {
    "results_per_page": int(os.getenv("UI_RESULTS_PER_PAGE", "5")),  # Количество результатов на страницу
    "max_inline_text_length": int(os.getenv("UI_MAX_INLINE_TEXT_LENGTH", "3500")),  # Максимальная длина текста в inline сообщении
}