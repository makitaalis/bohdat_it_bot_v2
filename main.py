#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram бот для поиска информации по VK ID и телефонным номерам
"""
import sys
import time
import asyncio
import traceback

import schedule
import threading
import aiogram.utils.exceptions
import re
import html
from typing import Dict, Any, List, Union, Optional, Tuple
from datetime import datetime

from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.callback_data import CallbackData
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile
)
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from config import TELEGRAM_TOKEN, BOT_MESSAGES, BOT_ADMINS, UI_SETTINGS
from logger import logger, log_user_action, log_error
from api_client import api_client
from formatter import formatter
from database import db
from file_processing import extract_vk_links, extract_vk_id, extract_phone_from_vk_parsing, create_results_file, \
    process_vk_links

# Dictionary for caching results
cached_results = {}
# Регулярное выражение для очистки кеша через интервалы времени
CACHE_CLEANUP_INTERVAL = 60 * 30  # 30 минут


class UserStates(StatesGroup):
    """Состояния пользователя для FSM"""
    waiting_for_vk_id = State()  # Ожидание VK ID
    waiting_for_phone = State()  # Ожидание телефонного номера
    waiting_for_settings = State()  # Ожидание изменения настроек


# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# Callback data для кнопок
settings_cb = CallbackData("settings", "action")
limit_cb = CallbackData("limit", "value")
lang_cb = CallbackData("lang", "value")
page_cb = CallbackData("page", "query_id", "page_id")


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    # Сохраняем информацию о пользователе
    db.save_user(user_id, username, first_name, last_name)

    # Логируем действие
    log_user_action(user_id, username, "started the bot")

    welcome_text = f"""
Привет, {first_name or 'друг'}! 👋

Я бот для поиска информации по VK ID и телефонным номерам.

📱 Вы можете отправить мне:
- VK ID (например, 12345678 или id12345678)
- Номер телефона (например, +79123456789)
- Текстовый файл со списком ссылок на профили ВКонтакте

🔍 Я найду информацию в базах данных и сообщу вам результаты.

📘 Используйте /help для получения справки.
⚙️ Используйте /settings для изменения настроек.
    """

    # Отправляем приветственное сообщение (без parse_mode)
    await message.answer(
        welcome_text,
        reply_markup=get_main_keyboard()
    )


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    """Обработчик команды /help"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Логируем действие
    log_user_action(user_id, username, "requested help")

    help_text = """
<b>📚 Справка по использованию бота</b>

<b>Основные возможности:</b>
- Поиск информации по VK ID 
- Поиск информации по номеру телефона
- Обработка файлов со списком VK профилей

<b>Как использовать:</b>
1. Отправьте мне VK ID (например, 12345678 или id12345678)
2. Или отправьте номер телефона (например, +79123456789)
3. Или используйте команду /process_file для обработки списка VK профилей
4. Я найду информацию и отправлю вам результаты

<b>Доступные команды:</b>
/start - Начать работу с ботом
/help - Показать эту справку
/vk [ID] - Поиск по VK ID
/phone [номер] - Поиск по номеру телефона
/process_file - Обработка файла со списком VK профилей
/api_status - Проверить доступность API сервера
/settings - Настройки бота
/status - Показать статус API (только для администраторов)

<b>Полезные советы:</b>
- Для получения наилучших результатов используйте полный формат VK ID
- Для телефонов можно использовать различные форматы, я распознаю номера автоматически
- Полный отчет доступен в формате HTML-файла
- При обработке файла каждая ссылка на профиль должна быть в формате https://vk.com/id123456
- Если API недоступен, используйте команду /api_status для проверки
    """

    # Отправляем справку с HTML форматированием
    await message.answer(
        help_text,
        parse_mode="HTML",
        reply_markup=get_main_keyboard()
    )


@dp.message_handler(commands=["vk"])
async def cmd_vk(message: types.Message):
    """Обработчик команды /vk"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Получаем VK ID из сообщения
    cmd_parts = message.get_args().split()

    if not cmd_parts:
        # Если ID не указан, переходим в режим ожидания ID
        await UserStates.waiting_for_vk_id.set()
        await message.answer(
            "🔍 Пожалуйста, отправьте VK ID для поиска.\n\n"
            "Вы можете отправить числовой ID или ID в формате 'id123456'.",
            reply_markup=get_cancel_keyboard()
        )
        return

    vk_id = cmd_parts[0]

    # Логируем действие
    log_user_action(user_id, username, "requested VK ID search", vk_id)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(
        BOT_MESSAGES["processing"],
        reply_markup=types.ReplyKeyboardRemove()
    )

    # Получаем настройки пользователя
    user_settings = db.get_user_settings(user_id)

    try:
        # Выполняем поиск по VK ID
        response = await process_vk_search(vk_id, user_id, user_settings)

        # Отправляем результаты
        await send_search_results(message.chat.id, vk_id, response, processing_msg.message_id, user_id)

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "vk_id": vk_id})

        # Отправляем сообщение об ошибке
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=BOT_MESSAGES["error"].format(error=str(e))
            )
        except aiogram.utils.exceptions.MessageNotModified:
            pass
        except Exception as edit_error:
            logger.error(f"Error editing error message: {edit_error}")
            await message.answer(BOT_MESSAGES["error"].format(error=str(e)))

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)  # Небольшая задержка для улучшения UX
        await message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )


@dp.message_handler(commands=["phone"])
async def cmd_phone(message: types.Message):
    """Обработчик команды /phone"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Получаем номер телефона из сообщения
    cmd_parts = message.get_args().split()

    if not cmd_parts:
        # Если номер не указан, переходим в режим ожидания номера
        await UserStates.waiting_for_phone.set()
        await message.answer(
            "📱 Пожалуйста, отправьте номер телефона для поиска.\n\n"
            "Вы можете отправить номер в любом формате, например, +79123456789 или 8-912-345-67-89.",
            reply_markup=get_cancel_keyboard()
        )
        return

    phone = cmd_parts[0]

    # Логируем действие
    log_user_action(user_id, username, "requested phone search", phone)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(
        "🔍 Ищу информацию по номеру телефона, пожалуйста, подождите...",
        reply_markup=types.ReplyKeyboardRemove()
    )

    try:
        # Выполняем поиск по номеру телефона
        results = await process_phone_search(phone, user_id)

        # Отправляем результаты
        await send_phone_results(message.chat.id, phone, results, processing_msg.message_id)

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "phone": phone})

        # Отправляем сообщение об ошибке
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=BOT_MESSAGES["error"].format(error=str(e))
            )
        except Exception as edit_error:
            logger.error(f"Error editing error message: {edit_error}")
            await message.answer(BOT_MESSAGES["error"].format(error=str(e)))

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)  # Небольшая задержка для улучшения UX
        await message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )


@dp.message_handler(commands=["process_file"])
async def cmd_process_file(message: types.Message):
    """Обработчик команды /process_file"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Логируем действие
    log_user_action(user_id, username, "requested file processing")

    # Prompt the user to upload a file
    await message.answer(
        "📂 Пожалуйста, загрузите текстовый файл (.txt) со ссылками на профили ВКонтакте.\n\n"
        "Каждая ссылка должна быть в формате https://vk.com/id123456 и находиться на отдельной строке.",
        reply_markup=get_cancel_keyboard()
    )


@dp.message_handler(content_types=types.ContentType.DOCUMENT)
async def handle_document(message: types.Message):
    """Обработчик загрузки документов"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Проверяем тип файла
    if not message.document.mime_type or 'text' not in message.document.mime_type:
        await message.answer("❌ Пожалуйста, загрузите текстовый файл (.txt)")
        return

    # Логируем действие
    log_user_action(user_id, username, "uploaded a file for processing")

    # Отправляем сообщение о начале загрузки файла
    download_msg = await message.answer("📥 Загружаю и анализирую файл...")

    # Загружаем файл
    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        downloaded_file = await bot.download_file(file_path)

        # Обрабатываем содержимое файла
        file_content = downloaded_file.read().decode('utf-8', errors='ignore')
    except Exception as e:
        log_error(e, {"user_id": user_id, "file_id": file_id if 'file_id' in locals() else "unknown"})
        await message.answer("❌ Ошибка при чтении файла. Проверьте формат файла и попробуйте снова.")
        return

    # Определяем тип данных в файле
    lines = [line.strip() for line in file_content.split('\n') if line.strip()]

    # Проверяем наличие VK ссылок
    vk_links = extract_vk_links(file_content)

    # Проверяем наличие запросов в формате ФИО + дата рождения
    name_dob_queries = []
    dob_pattern = re.compile(r'^\S+\s+\S+\s+\d{1,2}[./-]\d{1,2}[./-]\d{2,4}$')

    for line in lines:
        if dob_pattern.match(line) and line not in vk_links:
            name_dob_queries.append(line)

    # Определяем тип обработки
    if vk_links and not name_dob_queries:
        # Обработка VK ссылок
        await handle_vk_links_file(message, vk_links, user_id, download_msg)
    elif name_dob_queries and not vk_links:
        # Обработка запросов ФИО + дата рождения
        await handle_name_dob_file(message, name_dob_queries, user_id, download_msg)
    elif vk_links and name_dob_queries:
        # Файл содержит оба типа данных
        # Спрашиваем пользователя, какой тип обработки выполнить
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("🔍 Обработать как VK ссылки", callback_data=f"process_file:vk_links:{file_id}"),
            InlineKeyboardButton("👤 Обработать как ФИО + дата рождения",
                                 callback_data=f"process_file:name_dob:{file_id}")
        )

        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=download_msg.message_id,
            text=f"🤔 Обнаружено {len(vk_links)} VK ссылок и {len(name_dob_queries)} запросов ФИО + дата рождения.\n"
                 f"Выберите тип обработки:",
            reply_markup=markup
        )
    else:
        # Не удалось определить тип данных
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=download_msg.message_id,
            text="❌ В файле не найдено данных для обработки. Файл должен содержать:\n"
                 "- Ссылки на профили ВКонтакте (https://vk.com/id123456)\n"
                 "- Или запросы в формате 'Фамилия Имя ДД.ММ.ГГГГ'"
        )


async def handle_name_dob_file(message, queries, user_id, download_msg):
    """Обработчик файла с запросами ФИО + дата рождения"""
    # Ограничиваем количество запросов
    max_queries = 500
    if len(queries) > max_queries:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=download_msg.message_id,
            text=f"⚠️ Обнаружено слишком много запросов ({len(queries)}). "
                 f"Будут обработаны первые {max_queries}."
        )
        queries = queries[:max_queries]

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Обновляем сообщение о статусе анализа файла
    try:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=download_msg.message_id,
            text=f"🔍 Найдено {len(queries)} запросов в формате ФИО + дата рождения.\n"
                 f"⚙️ Начинаю пакетную обработку...\n\n"
                 f"ℹ️ <b>Обратите внимание:</b> Обработка может занять несколько минут.",
            parse_mode="HTML"
        )
    except Exception as e:
        log_error(e, {"action": "updating download message"})
        processing_msg = await message.answer(
            f"🔍 Найдено {len(queries)} запросов в формате ФИО + дата рождения.\n"
            f"⚙️ Начинаю пакетную обработку...\n\n"
            f"ℹ️ <b>Обратите внимание:</b> Обработка может занять несколько минут.",
            parse_mode="HTML"
        )
        download_msg = processing_msg

    # Добавляем небольшую задержку перед обработкой
    await asyncio.sleep(0.5)

    start_time = time.time()

    try:
        # Обрабатываем запросы
        results = await process_vk_links_advanced(
            queries,
            user_id,
            message.chat.id,
            download_msg.message_id,
            bot,
            db,
            is_name_dob_format=True  # Указываем, что это запросы ФИО + ДР
        )

        # Рассчитываем затраченное время
        processing_time = time.time() - start_time
        time_str = f"{int(processing_time // 60)} мин {int(processing_time % 60)} сек"

        # Создаем файл с результатами
        result_file_path = create_results_file(results)

        # Считаем статистику
        results_with_phones = len([r for r in results if r[1] and len(r[1]) > 0])
        total_phones = sum(len(phones) for _, phones in results if phones)

        # Отправляем файл с результатами
        caption = (
            f"✅ Пакетная обработка завершена за {time_str}.\n"
            f"📊 Статистика:\n"
            f"- Обработано запросов: {len(results)}\n"
            f"- Найдено запросов с номерами: {results_with_phones}/{len(results)}\n"
            f"- Всего найдено номеров: {total_phones}\n\n"
            f"📄 Результаты сохранены в Excel файле (.xlsx)."
        )

        await bot.send_document(
            chat_id=message.chat.id,
            document=InputFile(result_file_path),
            caption=caption,
            parse_mode="HTML"
        )

    except Exception as e:
        log_error(e, {"user_id": user_id, "action": "batch processing", "details": str(e)})
        await message.answer(
            f"❌ Ошибка при обработке файла: {str(e)}\n\n"
            f"Возможные причины:\n"
            f"- Проблемы с доступом к API\n"
            f"- Неверный формат данных\n"
            f"- Внутренняя ошибка сервера\n\n"
            f"Пожалуйста, попробуйте позже или обратитесь к администратору."
        )
    finally:
        # Возвращаем основную клавиатуру
        await message.answer("Готово! Чем еще я могу помочь?", reply_markup=get_main_keyboard())


async def process_vk_links_advanced(queries, user_id, chat_id, message_id, bot_instance, db_instance,
                                    is_name_dob_format=False, *args):
    """
    Обрабатывает список запросов - либо VK ссылки, либо запросы ФИО + дата рождения

    Args:
        queries (List[str]): Список запросов
        user_id (int): ID пользователя
        chat_id (int): ID чата
        message_id (int): ID сообщения для обновления
        bot_instance: Экземпляр бота
        db_instance: Экземпляр базы данных
        is_name_dob_format (bool): True если запросы в формате ФИО + ДР
        *args: Дополнительные аргументы

    Returns:
        List[Tuple[str, List[str]]]: Список кортежей (запрос, список телефонов)
    """
    from api_client import api_client  # Импортируем клиент API

    if not queries:
        return []

    results = []
    total = len(queries)

    # Get user settings
    user_settings = db_instance.get_user_settings(user_id)

    # Track successful and failed requests
    success_count = 0
    fail_count = 0

    # Флаг для отслеживания возможности редактирования сообщения
    can_edit_message = True
    update_message_id = message_id  # ID сообщения для обновления (может меняться, если создаем новое)

    # Тип данных для сообщений
    item_type = "запросов" if is_name_dob_format else "ссылок"

    # Добавляем небольшую задержку перед первым редактированием сообщения
    await asyncio.sleep(0.5)

    # Функция для безопасного обновления сообщения о прогрессе
    async def safe_update_progress(text):
        nonlocal can_edit_message, update_message_id

        if can_edit_message:
            try:
                await bot_instance.edit_message_text(
                    chat_id=chat_id,
                    message_id=update_message_id,
                    text=text
                )
            except (aiogram.utils.exceptions.MessageCantBeEdited,
                    aiogram.utils.exceptions.MessageNotModified,
                    aiogram.utils.exceptions.MessageToEditNotFound) as e:
                logger.warning(f"Cannot edit message: {e}. Will send new messages for progress updates.")
                can_edit_message = False
                # Отправляем новое сообщение вместо редактирования
                new_msg = await bot_instance.send_message(
                    chat_id=chat_id,
                    text=text
                )
                update_message_id = new_msg.message_id
            except Exception as e:
                logger.error(f"Error updating progress message: {e}")
                can_edit_message = False
        else:
            # Если редактирование невозможно, отправляем новое сообщение
            try:
                new_msg = await bot_instance.send_message(
                    chat_id=chat_id,
                    text=text
                )
                update_message_id = new_msg.message_id
            except Exception as e:
                logger.error(f"Error sending progress message: {e}")

    # Сообщаем о начале обработки
    await safe_update_progress(f"🔍 Подготовка к пакетной обработке {total} {item_type}...")

    # Обрабатываем каждый запрос
    for i, query in enumerate(queries):
        # Обновляем статус каждые 5 запросов
        if i % 5 == 0 or i == 0 or i == len(queries) - 1:
            try:
                await bot_instance.edit_message_text(
                    chat_id=chat_id,
                    message_id=update_message_id,
                    text=f"⏳ Обработка запроса {i + 1}/{len(queries)} ({(i + 1) / len(queries) * 100:.1f}%)..."
                )
            except Exception as e:
                logger.error(f"Ошибка при обновлении сообщения: {e}")

        try:
            if is_name_dob_format:
                # Выполняем комплексный поиск для запроса ФИО + ДР
                search_result = await advanced_search(query, user_id, user_settings)
                phones = search_result.get("phones", [])
                results.append((query, phones))
            else:
                # Обработка VK ссылок (старый вариант)
                vk_id = extract_vk_id(query)
                if vk_id:
                    response = await process_vk_search(vk_id, user_id, user_settings)
                    phones = extract_phone_from_vk_parsing(response, vk_id)
                    results.append((query, phones))
                else:
                    results.append((query, []))

            # Обновляем статистику
            if (is_name_dob_format and phones) or (not is_name_dob_format and vk_id and phones):
                success_count += 1
            else:
                fail_count += 1

            # Небольшая задержка между запросами
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Ошибка при обработке запроса {query}: {e}")
            results.append((query, []))
            fail_count += 1

    # Обновляем финальный статус
    phones_found = sum(1 for _, phones in results if phones and len(phones) > 0)
    total_phones = sum(len(phones) for _, phones in results if phones)

    await safe_update_progress(
        f"✅ Обработка завершена: {len(queries)} {item_type} (100%)\n"
        f"✅ Успешно: {success_count} | ❌ Ошибок: {fail_count}\n"
        f"📱 Найдено {phones_found} записей с номерами (всего {total_phones} номеров)"
    )

    return results

@dp.callback_query_handler(lambda c: c.data.startswith('process_file:'))
async def process_file_callback(call: types.CallbackQuery):
    """Обработчик выбора типа обработки файла"""
    user_id = call.from_user.id

    # Разбираем callback data
    parts = call.data.split(':')
    if len(parts) < 3:
        await call.answer("Некорректный формат данных", show_alert=True)
        return

    process_type = parts[1]
    file_id = parts[2]

    # Скачиваем файл
    try:
        file = await bot.get_file(file_id)
        file_path = file.file_path
        downloaded_file = await bot.download_file(file_path)
        file_content = downloaded_file.read().decode('utf-8', errors='ignore')
    except Exception as e:
        log_error(e, {"user_id": user_id, "file_id": file_id})
        await call.message.answer("❌ Ошибка при чтении файла. Возможно, файл был удален.")
        await call.answer()
        return

    # Обновляем сообщение
    processing_msg = await call.message.edit_text("🔄 Начинаю обработку файла...")

    # Обрабатываем файл в зависимости от выбранного типа
    if process_type == "vk_links":
        # Извлекаем VK ссылки
        links = extract_vk_links(file_content)
        # Обрабатываем как VK ссылки
        await handle_vk_links_file(call.message, links, user_id, processing_msg)
    elif process_type == "name_dob":
        # Извлекаем запросы ФИО + дата рождения
        lines = [line.strip() for line in file_content.split('\n') if line.strip()]
        dob_pattern = re.compile(r'^\S+\s+\S+\s+\d{1,2}[./-]\d{1,2}[./-]\d{2,4}$')
        queries = [line for line in lines if dob_pattern.match(line)]

        # Обрабатываем как запросы ФИО + дата рождения
        await handle_name_dob_file(call.message, queries, user_id, processing_msg)

    await call.answer()

@dp.message_handler(state=UserStates.waiting_for_vk_id)
async def process_vk_id_input(message: types.Message, state: FSMContext):
    """Обработчик ввода VK ID в режиме ожидания"""
    user_id = message.from_user.id
    username = message.from_user.username
    vk_id = message.text.strip()

    # Проверяем на команду отмены
    if vk_id.lower() == "отмена" or vk_id == "/cancel":
        await state.finish()
        await message.answer(
            "🚫 Поиск отменен. Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )
        return

    # Выходим из состояния ожидания
    await state.finish()

    # Логируем действие
    log_user_action(user_id, username, "submitted VK ID", vk_id)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(
        BOT_MESSAGES["processing"],
        reply_markup=types.ReplyKeyboardRemove()
    )

    # Получаем настройки пользователя
    user_settings = db.get_user_settings(user_id)

    try:
        # Выполняем поиск по VK ID
        response = await process_vk_search(vk_id, user_id, user_settings)

        # Отправляем результаты
        try:
            await send_search_results(message.chat.id, vk_id, response, processing_msg.message_id, user_id)
        except aiogram.utils.exceptions.MessageCantBeEdited as e:
            # Дополнительный уровень обработки ошибки, если send_search_results не смог её обработать
            log_error(e, {"user_id": user_id, "vk_id": vk_id})

            formatted_message = formatter.format_telegram_message(response, vk_id)
            try:
                await message.answer(
                    formatted_message,
                    parse_mode="HTML"
                )
            except Exception as html_error:
                logger.error(f"Error sending HTML message: {html_error}")
                # В крайнем случае отправляем без форматирования
                await message.answer(
                    html.unescape(formatted_message).replace("<b>", "").replace("</b>", "").replace("<i>", "").replace(
                        "</i>", "")
                )

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "vk_id": vk_id})

        # Отправляем сообщение об ошибке
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=BOT_MESSAGES["error"].format(error=str(e))
            )
        except (aiogram.utils.exceptions.MessageCantBeEdited,
                aiogram.utils.exceptions.MessageNotModified,
                aiogram.utils.exceptions.MessageToEditNotFound) as edit_error:
            # Если не можем отредактировать, отправляем новое сообщение
            logger.warning(f"Could not edit error message, sending new one: {edit_error}")
            await message.answer(
                BOT_MESSAGES["error"].format(error=str(e))
            )

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)  # Небольшая задержка для улучшения UX
        await message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )


@dp.message_handler(state=UserStates.waiting_for_phone)
async def process_phone_input(message: types.Message, state: FSMContext):
    """Обработчик ввода номера телефона в режиме ожидания"""
    user_id = message.from_user.id
    username = message.from_user.username
    phone = message.text.strip()

    # Проверяем на команду отмены
    if phone.lower() == "отмена" or phone == "/cancel":
        await state.finish()
        await message.answer(
            "🚫 Поиск отменен. Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )
        return

    # Выходим из состояния ожидания
    await state.finish()

    # Логируем действие
    log_user_action(user_id, username, "submitted phone number", phone)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(
        "🔍 Ищу информацию по номеру телефона, пожалуйста, подождите...",
        reply_markup=types.ReplyKeyboardRemove()
    )

    try:
        # Выполняем поиск по номеру телефона
        results = await process_phone_search(phone, user_id)

        # Отправляем результаты
        await send_phone_results(message.chat.id, phone, results, processing_msg.message_id)

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "phone": phone})

        # Отправляем сообщение об ошибке
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=BOT_MESSAGES["error"].format(error=str(e))
            )
        except Exception as edit_error:
            logger.error(f"Error editing error message: {edit_error}")
            await message.answer(BOT_MESSAGES["error"].format(error=str(e)))

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)  # Небольшая задержка для улучшения UX
        await message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )


@dp.message_handler(commands=["settings"])
async def cmd_settings(message: types.Message):
    """Обработчик команды /settings"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Логируем действие
    log_user_action(user_id, username, "opened settings")

    # Получаем настройки пользователя
    user_settings = db.get_user_settings(user_id)

    # Формируем сообщение с текущими настройками
    settings_text = (
        "<b>⚙️ Текущие настройки:</b>\n\n"
        f"🌐 Язык результатов: <code>{user_settings.get('language', 'ru')}</code>\n"
        f"🔢 Результатов на страницу: <code>{user_settings.get('results_per_page', 5)}</code>\n\n"
        "Выберите настройку для изменения:"
    )

    # Создаем клавиатуру с кнопками настроек
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("🌐 Изменить язык", callback_data=settings_cb.new(action="change_lang")),
        InlineKeyboardButton("🔢 Изменить количество результатов", callback_data=settings_cb.new(action="change_limit"))
    )

    await message.answer(settings_text, parse_mode="HTML", reply_markup=markup)


@dp.callback_query_handler(settings_cb.filter(action="change_lang"))
async def change_language(call: types.CallbackQuery, callback_data: Dict[str, str]):
    """Обработчик кнопки изменения языка"""
    # Создаем клавиатуру с доступными языками
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🇷🇺 Русский", callback_data=lang_cb.new(value="ru")),
        InlineKeyboardButton("🇬🇧 English", callback_data=lang_cb.new(value="en"))
    )

    await call.message.edit_text(
        "Выберите язык для результатов поиска:",
        reply_markup=markup
    )
    await call.answer()


@dp.callback_query_handler(lang_cb.filter())
async def set_language(call: types.CallbackQuery, callback_data: Dict[str, str]):
    """Обработчик выбора языка"""
    user_id = call.from_user.id
    username = call.from_user.username
    lang = callback_data["value"]

    # Обновляем настройки пользователя
    db.update_user_settings(user_id, {"language": lang})

    # Логируем действие
    log_user_action(user_id, username, f"changed language to {lang}")

    # Получаем обновленные настройки
    user_settings = db.get_user_settings(user_id)

    # Формируем сообщение с текущими настройками
    settings_text = (
        "<b>✅ Настройки обновлены:</b>\n\n"
        f"🌐 Язык результатов: <code>{user_settings.get('language', 'ru')}</code>\n"
        f"🔢 Результатов на страницу: <code>{user_settings.get('results_per_page', 5)}</code>\n"
    )

    await call.message.edit_text(
        settings_text,
        parse_mode="HTML"
    )
    await call.answer(BOT_MESSAGES["settings_updated"])

@dp.message_handler(lambda message: message.text == "🔍 Поиск по ФИО + ДР")
async def button_search_name_dob(message: types.Message):
    """Обработчик кнопки поиска по ФИО + дате рождения"""
    await message.answer(
        "🔍 Введите данные для поиска в формате:\n"
        "<b>Фамилия Имя ДД.ММ.ГГГГ</b>\n\n"
        "Например: <code>Иванов Иван 01.01.2000</code>",
        parse_mode="HTML",
        reply_markup=get_cancel_keyboard()
    )

@dp.callback_query_handler(settings_cb.filter(action="change_limit"))
async def change_limit(call: types.CallbackQuery, callback_data: Dict[str, str]):
    """Обработчик кнопки изменения лимита результатов на страницу"""
    # Создаем клавиатуру с доступными вариантами
    markup = InlineKeyboardMarkup(row_width=3)
    markup.add(
        InlineKeyboardButton("3", callback_data=limit_cb.new(value="3")),
        InlineKeyboardButton("5", callback_data=limit_cb.new(value="5")),
        InlineKeyboardButton("10", callback_data=limit_cb.new(value="10"))
    )

    await call.message.edit_text(
        "Выберите количество результатов на страницу:",
        reply_markup=markup
    )
    await call.answer()


@dp.callback_query_handler(limit_cb.filter())
async def set_limit(call: types.CallbackQuery, callback_data: Dict[str, str]):
    """Обработчик выбора лимита результатов"""
    user_id = call.from_user.id
    username = call.from_user.username
    limit = int(callback_data["value"])

    # Обновляем настройки пользователя
    db.update_user_settings(user_id, {"results_per_page": limit})

    # Логируем действие
    log_user_action(user_id, username, f"changed results_per_page to {limit}")

    # Получаем обновленные настройки
    user_settings = db.get_user_settings(user_id)

    # Формируем сообщение с текущими настройками
    settings_text = (
        "<b>✅ Настройки обновлены:</b>\n\n"
        f"🌐 Язык результатов: <code>{user_settings.get('language', 'ru')}</code>\n"
        f"🔢 Результатов на страницу: <code>{user_settings.get('results_per_page', 5)}</code>\n"
    )

    await call.message.edit_text(
        settings_text,
        parse_mode="HTML"
    )
    await call.answer(BOT_MESSAGES["settings_updated"])


@dp.message_handler(commands=["status"])
async def cmd_status(message: types.Message):
    """Обработчик команды /status"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Проверяем, является ли пользователь администратором
    if user_id not in BOT_ADMINS:
        await message.answer("⛔ У вас нет доступа к этой команде.")
        return

    # Логируем действие
    log_user_action(user_id, username, "requested status")

    # Получаем статистику кеша
    cache_stats = db.get_cache_stats()

    # Формируем сообщение со статусом
    status_text = (
        "<b>📊 Статус системы:</b>\n\n"
        f"🗃 Записей в кеше: <code>{cache_stats['total_entries']}</code>\n"
        f"📱 Записей с телефонами: <code>{cache_stats['phone_entries']}</code>\n"
        f"📊 Средний размер ответа: <code>{int(cache_stats['avg_response_size'] or 0)} байт</code>\n"
        f"☎️ Всего телефонных номеров в базе: <code>{cache_stats['total_phones']}</code>\n"
        f"👤 Уникальных VK ID с телефонами: <code>{cache_stats['unique_vk_ids']}</code>\n\n"
        "<b>Популярные запросы:</b>\n"
    )

    # Добавляем популярные запросы
    if cache_stats['popular_queries']:
        for query in cache_stats['popular_queries']:
            status_text += f"- <code>{html.escape(query['query'])}</code> ({query['hit_count']} запросов)\n"
    else:
        status_text += "Нет данных о популярных запросах\n"

    await message.answer(status_text, parse_mode="HTML")


async def check_api_status():
    """
    Проверяет доступность и работоспособность API

    Returns:
        Tuple[bool, str]: (статус, сообщение) - True если API доступен, иначе False
    """
    from api_client import api_client

    try:
        # Делаем простой запрос для проверки доступности API
        # Используем короткий таймаут для быстрой проверки
        test_response = await asyncio.to_thread(
            api_client.make_request,
            query="test_connection",  # Простой запрос для проверки
            limit=10,  # Минимальный лимит
            max_retries=1  # Только одна попытка для быстрого ответа
        )

        # Проверяем результат
        if "error" in test_response:
            error_msg = test_response["error"]
            if "тайм-аут" in error_msg.lower():
                return False, "API не отвечает (тайм-аут)"
            elif "500" in error_msg:
                return False, "API недоступен (ошибка сервера 500)"
            else:
                return False, f"API недоступен: {error_msg}"

        # Если нет ошибки, API доступен
        return True, "API доступен и работает нормально"

    except Exception as e:
        logger.error(f"Error checking API status: {e}")
        return False, f"Ошибка при проверке статуса API: {str(e)}"


@dp.message_handler(commands=["api_status"])
async def cmd_api_status(message: types.Message):
    """Обработчик команды /api_status - проверяет доступность API"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Логируем действие
    log_user_action(user_id, username, "requested API status check")

    # Отправляем сообщение о начале проверки
    status_msg = await message.answer("🔄 Проверяю состояние API...")

    # Проверяем статус API
    api_available, status_message = await check_api_status()

    # Форматируем сообщение о статусе
    if api_available:
        status_text = f"✅ <b>API доступен</b>\n\n{status_message}\n\nМожно безопасно использовать функции поиска и пакетной обработки."
    else:
        status_text = f"❌ <b>API недоступен</b>\n\n{status_message}\n\nРекомендуется повторить попытку позже."

    # Обновляем сообщение
    try:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=status_msg.message_id,
            text=status_text,
            parse_mode="HTML"
        )
    except Exception as e:
        log_error(e, {"action": "updating API status message"})
        await message.answer(status_text, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data.startswith('download_html:'))
async def download_html_file(call: types.CallbackQuery):
    """Обработчик запроса на скачивание HTML файла"""
    user_id = call.from_user.id
    username = call.from_user.username

    # Получаем имя файла из callback data
    filename = call.data.split(':', 1)[1]
    file_path = f"temp/{filename}"

    try:
        # Отправляем файл пользователю
        await bot.send_document(
            chat_id=call.message.chat.id,
            document=InputFile(file_path),
            caption="📄 Полный отчет по вашему запросу"
        )

        # Логируем действие
        log_user_action(user_id, username, "downloaded HTML report", filename)

        await call.answer("✅ Файл отправлен")
    except Exception as e:
        log_error(e, {"user_id": user_id, "filename": filename})
        await call.answer("❌ Ошибка при отправке файла", show_alert=True)


@dp.callback_query_handler(lambda c: c.data.startswith('search_vk:'))
async def handle_search_vk_from_phone(call: types.CallbackQuery):
    """Обработчик поиска по VK ID из результатов поиска по телефону"""
    user_id = call.from_user.id
    username = call.from_user.username

    # Получаем VK ID из callback data
    vk_id = call.data.split(':', 1)[1]

    # Логируем действие
    log_user_action(user_id, username, "searched VK ID from phone results", vk_id)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await call.message.answer(
        BOT_MESSAGES["processing"],
        reply_markup=types.ReplyKeyboardRemove()
    )

    # Получаем настройки пользователя
    user_settings = db.get_user_settings(user_id)

    try:
        # Выполняем поиск по VK ID
        response = await process_vk_search(vk_id, user_id, user_settings)

        # Отправляем результаты
        await send_search_results(call.message.chat.id, vk_id, response, processing_msg.message_id, user_id)

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "vk_id": vk_id})

        # Отправляем сообщение об ошибке
        try:
            await bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=processing_msg.message_id,
                text=BOT_MESSAGES["error"].format(error=str(e))
            )
        except Exception as edit_error:
            logger.error(f"Error editing error message: {edit_error}")
            await call.message.answer(BOT_MESSAGES["error"].format(error=str(e)))

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)  # Небольшая задержка для улучшения UX
        await call.message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )


@dp.callback_query_handler(page_cb.filter())
async def handle_pagination(call: types.CallbackQuery, callback_data: Dict[str, str]):
    """Обработчик пагинации"""
    try:
        query_id = callback_data["query_id"]
        page_id = int(callback_data["page_id"])

        # Проверяем, есть ли данные
        if query_id not in cached_results:
            await call.answer("⚠️ Данные устарели или не найдены", show_alert=True)
            return

        results = cached_results[query_id]
        total_pages = len(results)

        # Проверяем диапазон страниц
        if page_id < 0:
            page_id = 0
        elif page_id >= total_pages:
            page_id = total_pages - 1

        # Создаем клавиатуру для пагинации
        markup = create_pagination_keyboard(query_id, page_id, total_pages)

        try:
            # Обновляем сообщение с новой страницей результатов
            await call.message.edit_text(
                text=results[page_id],
                parse_mode="HTML",
                reply_markup=markup
            )
        except aiogram.utils.exceptions.MessageNotModified:
            # Игнорируем ошибку, если текст не изменился
            pass
        except Exception:
            # Если не получается использовать HTML-форматирование, пробуем без него
            await call.message.edit_text(
                text=html.unescape(results[page_id]).replace("<b>", "").replace("</b>", "").replace("<i>", "").replace(
                    "</i>", ""),
                reply_markup=markup
            )

        await call.answer()

    except Exception as e:
        log_error(e, {"callback_data": callback_data})
        await call.answer("❌ Произошла ошибка при навигации", show_alert=True)


@dp.message_handler(content_types=types.ContentType.CONTACT)
async def handle_contact(message: types.Message):
    """Обработчик отправки контакта"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Получаем номер телефона из контакта
    phone = message.contact.phone_number

    # Логируем действие
    log_user_action(user_id, username, "shared contact", phone)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(
        "🔍 Ищу информацию по номеру телефона из контакта, пожалуйста, подождите...",
        reply_markup=types.ReplyKeyboardRemove()
    )

    try:
        # Выполняем поиск по номеру телефона
        results = await process_phone_search(phone, user_id)

        # Отправляем результаты
        await send_phone_results(message.chat.id, phone, results, processing_msg.message_id)

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "phone": phone})

        # Отправляем сообщение об ошибке
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=BOT_MESSAGES["error"].format(error=str(e))
            )
        except Exception as edit_error:
            logger.error(f"Error editing error message: {edit_error}")
            await message.answer(BOT_MESSAGES["error"].format(error=str(e)))

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)  # Небольшая задержка для улучшения UX
        await message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )


@dp.message_handler(lambda message: message.text == "🔍 Поиск по VK ID")
async def button_search(message: types.Message):
    """Обработчик кнопки поиска по VK ID"""
    # Переходим в режим ожидания VK ID
    await UserStates.waiting_for_vk_id.set()
    await message.answer(
        "🔍 Пожалуйста, отправьте VK ID для поиска.\n\n"
        "Вы можете отправить числовой ID или ID в формате 'id123456'.",
        reply_markup=get_cancel_keyboard()
    )


@dp.message_handler(lambda message: message.text == "📱 Поиск по телефону")
async def button_search_phone(message: types.Message):
    """Обработчик кнопки поиска по телефону"""
    # Переходим в режим ожидания номера телефона
    await UserStates.waiting_for_phone.set()

    # Создаем клавиатуру с кнопкой отправки контакта
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(KeyboardButton("📱 Отправить контакт", request_contact=True))
    keyboard.add(KeyboardButton("Отмена"))

    await message.answer(
        "📱 Пожалуйста, отправьте номер телефона для поиска.\n\n"
        "Вы можете отправить номер в любом формате или поделиться контактом из вашей телефонной книги.",
        reply_markup=keyboard
    )


@dp.message_handler(lambda message: message.text == "📂 Обработка файла")
async def button_process_file(message: types.Message):
    """Обработчик кнопки обработки файла"""
    await cmd_process_file(message)


@dp.message_handler(lambda message: message.text == "⚙️ Настройки")
async def button_settings(message: types.Message):
    """Обработчик кнопки настроек"""
    await cmd_settings(message)


@dp.message_handler(lambda message: message.text == "❓ Помощь")
async def button_help(message: types.Message):
    """Обработчик кнопки помощи"""
    await cmd_help(message)


@dp.message_handler(lambda message: message.text.lower() == "отмена", state="*")
async def button_cancel(message: types.Message, state: FSMContext):
    """Обработчик кнопки отмены"""
    current_state = await state.get_state()
    if current_state is not None:
        await state.finish()

    await message.answer(
        "🚫 Операция отменена. Чем я могу вам помочь?",
        reply_markup=get_main_keyboard()
    )


@dp.message_handler()
async def handle_message(message: types.Message):
    """Обработчик текстовых сообщений, которые не попали в другие обработчики"""
    text = message.text.strip()

    # Проверяем, похож ли запрос на "Фамилия Имя ДД.ММ.ГГГГ"
    parts = text.split()
    is_name_dob_query = False

    if len(parts) >= 3:
        # Проверяем, является ли последняя часть датой
        dob_pattern = re.compile(r'^\d{1,2}[./-]\d{1,2}[./-]\d{2,4}$')
        if dob_pattern.match(parts[-1]):
            is_name_dob_query = True

    if is_name_dob_query:
        # Эмулируем команду /search
        message.text = f"/search {text}"
        await cmd_search(message)
        return

    # Проверяем, является ли сообщение VK ID
    if _validate_vk_id(text):
        # Эмулируем команду /vk
        message.text = f"/vk {text}"
        await cmd_vk(message)
        return

    # Проверяем, является ли сообщение номером телефона
    digits = ''.join(c for c in text if c.isdigit())
    if len(digits) >= 7:
        # Эмулируем команду /phone
        message.text = f"/phone {text}"
        await cmd_phone(message)
        return

    # Если не распознали как команду или данные для поиска
    await message.answer(
        "Не уверен, что вы имеете в виду. Вы можете:\n"
        "- Отправить запрос в формате 'Фамилия Имя ДД.ММ.ГГГГ'\n"
        "- Отправить VK ID (например, 123456789 или id123456789)\n"
        "- Отправить номер телефона (например, +7 123 456 78 90)\n"
        "- Отправить файл со списком ссылок на профили ВКонтакте\n"
        "- Выбрать действие из меню ниже",
        reply_markup=get_main_keyboard()
    )


@dp.message_handler(commands=["search"])
async def cmd_search(message: types.Message):
    """Обработчик команды /search для поиска по ФИО и дате рождения"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Получаем запрос из сообщения
    cmd_parts = message.get_args().split()

    if not cmd_parts or len(cmd_parts) < 3:  # Минимум Фамилия, Имя и дата рождения
        await message.answer(
            "🔍 Для поиска укажите Фамилию, Имя и дату рождения в формате:\n"
            "/search Фамилия Имя ДД.ММ.ГГГГ\n\n"
            "Например: /search Иванов Иван 01.01.2000",
            reply_markup=get_main_keyboard()
        )
        return

    # Собираем запрос из аргументов
    query = " ".join(cmd_parts)

    # Логируем действие
    log_user_action(user_id, username, "requested advanced search", query)

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Отправляем сообщение о начале обработки
    processing_msg = await message.answer(
        "🔍 Выполняю комплексный поиск, пожалуйста, подождите...",
        reply_markup=types.ReplyKeyboardRemove()
    )

    # Получаем настройки пользователя
    user_settings = db.get_user_settings(user_id)

    try:
        # Логируем стандартизированные данные для запроса
        from name_dob_search import standardize_russian_name, standardize_birth_date, \
            search_phone_by_name_and_birth_date
        from api_client import api_client  # Импортируем API клиент

        name_parts = query.split()
        if len(name_parts) >= 3:
            date_part = name_parts[-1]
            name_part = " ".join(name_parts[:-1])
            name_data = standardize_russian_name(name_part)
            birth_date = standardize_birth_date(date_part)
            logger.info(f"Стандартизированные данные для поиска: {name_data}, дата: {birth_date}")

            # Выполняем комплексный поиск - правильный вызов функции
            results = await search_phone_by_name_and_birth_date(name_part, date_part, api_client)
        else:
            # Если формат некорректный, возвращаем пустой результат
            results = {
                "query": query,
                "phones": [],
                "method": "unknown",
                "confidence": 0.0,
                "error": "Неверный формат запроса"
            }

        # Формируем сообщение с результатами
        phones = results.get("phones", [])
        confidence = results.get("confidence", 0.0)
        method = results.get("method", "unknown")

        # Дополнительное логирование результатов
        logger.info(f"Результаты поиска: найдено {len(phones)} телефонов, метод: {method}, уверенность: {confidence}")

        # Проверяем, найдены ли телефоны
        if phones:
            # Формируем текст сообщения с найденными телефонами
            message_text = (
                f"✅ <b>Результаты поиска по запросу:</b> {html.escape(query)}\n\n"
                f"📱 <b>Найдено телефонов:</b> {len(phones)}\n"
                f"🔍 <b>Метод поиска:</b> {method_to_text(method)}\n"
                f"📊 <b>Уверенность:</b> {int(confidence * 100)}%\n\n"
                f"<b>Телефоны:</b>\n"
            )

            # Добавляем список телефонов
            for i, phone in enumerate(phones, 1):
                message_text += f"{i}. <code>{phone}</code>\n"
        else:
            # Если телефоны не найдены
            error_message = results.get("error", "Телефоны не найдены")
            message_text = f"ℹ️ {error_message} для запроса: {query}"

        # Безопасно отправляем результаты
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=message_text,
                parse_mode="HTML"
            )
        except (aiogram.utils.exceptions.MessageCantBeEdited,
                aiogram.utils.exceptions.MessageNotModified,
                aiogram.utils.exceptions.CantParseEntities) as e:
            # Если не можем редактировать, отправляем новое сообщение
            logger.warning(f"Не удалось редактировать сообщение: {e}. Отправляем новое.")
            try:
                await message.answer(
                    text=message_text,
                    parse_mode="HTML"
                )
            except aiogram.utils.exceptions.CantParseEntities:
                # Если проблема с HTML форматированием
                plain_text = message_text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>",
                                                                                                            "").replace(
                    "<code>", "").replace("</code>", "")
                await message.answer(plain_text)

    except Exception as e:
        # Логируем ошибку
        log_error(e, {"user_id": user_id, "query": query})
        logger.error(f"Полная ошибка при поиске: {str(e)}\n{traceback.format_exc()}")

        # Отправляем сообщение об ошибке
        error_message = BOT_MESSAGES["error"].format(error=str(e))
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                text=error_message
            )
        except (aiogram.utils.exceptions.MessageCantBeEdited,
                aiogram.utils.exceptions.MessageNotModified) as edit_error:
            # Если не можем редактировать, отправляем новое сообщение
            logger.warning(f"Не удалось редактировать сообщение с ошибкой: {edit_error}. Отправляем новое.")
            await message.answer(error_message)
        except Exception as edit_error:
            logger.error(f"Error handling error message: {edit_error}")
            await message.answer(error_message)

    finally:
        # Возвращаем основную клавиатуру
        await asyncio.sleep(1)
        await message.answer(
            "Готово! Чем еще я могу помочь?",
            reply_markup=get_main_keyboard()
        )

@dp.message_handler(commands=["process_name_dob"])
async def cmd_process_name_dob(message: types.Message):
    """Обработчик команды /process_name_dob для пакетной обработки запросов ФИО + дата рождения"""
    user_id = message.from_user.id
    username = message.from_user.username

    # Логируем действие
    log_user_action(user_id, username, "requested name+dob batch processing")

    # Запрашиваем файл
    await message.answer(
        "📂 Пожалуйста, загрузите текстовый файл (.txt) с запросами в формате 'Фамилия Имя ДД.ММ.ГГГГ'.\n\n"
        "Каждый запрос должен быть на отдельной строке, например:\n"
        "Иванов Иван 01.01.2000\n"
        "Петров Петр 15.05.2001",
        reply_markup=get_cancel_keyboard()
    )

def method_to_text(method, *args):
    """
    Преобразует метод поиска в читаемый текст

    Args:
        method (str): Метод поиска
        *args: Дополнительные аргументы

    Returns:
        str: Текстовое описание метода
    """
    methods = {
        "name_dob_search": "Поиск по ФИО и дате рождения",
        "email_search": "Поиск по email",
        "vk_search": "Поиск по VK ID",
        "unknown": "Комбинированный поиск"
    }
    return methods.get(method, "Неизвестный метод")

async def process_vk_search(vk_id: str, user_id: int, user_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Выполнение поиска по VK ID

    Args:
        vk_id (str): VK ID для поиска
        user_id (int): ID пользователя
        user_settings (Dict[str, Any]): Настройки пользователя

    Returns:
        Dict[str, Any]: Результаты поиска
    """
    # Получаем настройки пользователя
    lang = user_settings.get("language", "ru")

    # Фоновый запуск поиска (для асинхронности)
    loop = asyncio.get_event_loop()
    start_time = time.time()

    # Проверяем формат VK ID
    if not _validate_vk_id(vk_id):
        return {"error": "Неверный формат VK ID. Используйте числовой ID или формат 'id123456'"}

    # Пытаемся получить результаты из кеша или API
    response = await loop.run_in_executor(
        None,
        lambda: api_client.search_vk_id(vk_id, lang=lang)
    )

    processing_time = time.time() - start_time

    # Определяем, был ли ответ получен из кеша
    cached = "error" not in response and "_meta" in response and response["_meta"].get("source") == "cache"

    # Логируем запрос в базу данных
    db.log_query(
        user_id=user_id,
        query=vk_id,
        api_called=not cached,
        cached=cached,
        status_code=200 if "error" not in response else 400,
        response_size=len(str(response)),
        processing_time=processing_time
    )

    return response


async def process_phone_search(phone: str, user_id: int) -> List[Dict[str, Any]]:
    """
    Выполнение поиска по номеру телефона в локальной базе данных

    Args:
        phone (str): Номер телефона для поиска
        user_id (int): ID пользователя

    Returns:
        List[Dict[str, Any]]: Результаты поиска
    """
    # Нормализуем телефонный номер
    normalized_phone = ''.join(c for c in phone if c.isdigit())

    if len(normalized_phone) < 7:
        return []  # Слишком короткий номер, не ищем

    # Фоновый запуск поиска (для асинхронности)
    loop = asyncio.get_event_loop()
    start_time = time.time()

    # Пытаемся получить результаты из локальной базы
    results = await loop.run_in_executor(
        None,
        lambda: db.search_phone_number(normalized_phone)
    )

    processing_time = time.time() - start_time

    # Логируем запрос в базу данных
    db.log_query(
        user_id=user_id,
        query=f"phone:{normalized_phone}",
        api_called=False,
        cached=True,
        status_code=200,
        response_size=len(str(results)),
        processing_time=processing_time
    )

    return results


async def send_search_results(chat_id: int, vk_id: str, response: Dict[str, Any], processing_msg_id: int = None,
                              user_id: int = None):
    """
    Отправка результатов поиска по VK ID пользователю

    Args:
        chat_id (int): ID чата для отправки
        vk_id (str): VK ID для которого был сделан запрос
        response (Dict[str, Any]): Ответ API
        processing_msg_id (int, optional): ID сообщения о начале обработки
        user_id (int, optional): ID пользователя для логирования
    """
    # Если есть ошибка, отправляем сообщение об ошибке
    if "error" in response:
        error_message = BOT_MESSAGES["error"].format(error=response["error"])

        if processing_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=processing_msg_id,
                    text=error_message
                )
            except (aiogram.utils.exceptions.MessageCantBeEdited,
                    aiogram.utils.exceptions.MessageNotModified,
                    aiogram.utils.exceptions.MessageToEditNotFound) as e:
                # Если не можем отредактировать, отправляем новое сообщение
                logger.warning(f"Could not edit message, sending new one: {e}")
                await bot.send_message(
                    chat_id=chat_id,
                    text=error_message
                )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=error_message
            )
        return

    # Если нет результатов, отправляем сообщение об этом
    if "List" not in response or not response["List"] or list(response["List"].keys()) == ["No results found"]:
        no_results_message = BOT_MESSAGES["no_results"]

        if processing_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=processing_msg_id,
                    text=no_results_message
                )
            except (aiogram.utils.exceptions.MessageCantBeEdited,
                    aiogram.utils.exceptions.MessageNotModified,
                    aiogram.utils.exceptions.MessageToEditNotFound) as e:
                # Если не можем отредактировать, отправляем новое сообщение
                logger.warning(f"Could not edit message, sending new one: {e}")
                await bot.send_message(
                    chat_id=chat_id,
                    text=no_results_message
                )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=no_results_message
            )
        return

    # Форматируем сообщение для Telegram с HTML-форматированием
    telegram_message = formatter.format_telegram_message(response, vk_id)

    # Генерируем HTML файл
    html_content = formatter.format_html(response, vk_id)
    file_path = formatter.save_html_file(html_content, vk_id)

    # Создаем клавиатуру для отправки HTML файла
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        "📄 Скачать полный отчет (HTML)",
        callback_data=f"download_html:{file_path.name}"
    ))

    # Отправляем сообщение с результатами
    if processing_msg_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=processing_msg_id,
                text=telegram_message,
                parse_mode="HTML",
                reply_markup=markup
            )
        except (aiogram.utils.exceptions.MessageCantBeEdited,
                aiogram.utils.exceptions.MessageNotModified,
                aiogram.utils.exceptions.MessageToEditNotFound,
                aiogram.utils.exceptions.CantParseEntities) as e:
            # Если не можем отредактировать или возникла ошибка форматирования, отправляем новое сообщение
            logger.warning(f"Could not edit message, sending new one: {e}")
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=telegram_message,
                    parse_mode="HTML",
                    reply_markup=markup
                )
            except aiogram.utils.exceptions.CantParseEntities:
                # Если HTML не работает, отправляем без форматирования
                plain_text = html.unescape(telegram_message).replace("<b>", "").replace("</b>", "").replace("<i>",
                                                                                                            "").replace(
                    "</i>", "").replace("<code>", "").replace("</code>", "")
                await bot.send_message(
                    chat_id=chat_id,
                    text=plain_text,
                    reply_markup=markup
                )
        except Exception as e:
            logger.error(f"Unhandled error when sending search results: {e}")
            try:
                # Последняя попытка: простой текст без форматирования
                plain_text = html.unescape(telegram_message).replace("<b>", "").replace("</b>", "").replace("<i>",
                                                                                                            "").replace(
                    "</i>", "").replace("<code>", "").replace("</code>", "")
                await bot.send_message(
                    chat_id=chat_id,
                    text=plain_text,
                    reply_markup=markup
                )
            except Exception as last_error:
                logger.error(f"Failed to send results after all attempts: {last_error}")
    else:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=telegram_message,
                parse_mode="HTML",
                reply_markup=markup
            )
        except aiogram.utils.exceptions.CantParseEntities:
            # Если HTML не работает, отправляем без форматирования
            plain_text = html.unescape(telegram_message).replace("<b>", "").replace("</b>", "").replace("<i>",
                                                                                                        "").replace(
                "</i>", "").replace("<code>", "").replace("</code>", "")
            await bot.send_message(
                chat_id=chat_id,
                text=plain_text,
                reply_markup=markup
            )


async def handle_vk_links_file(message, links, user_id, download_msg):
    """
    Обработчик файла с VK ссылками

    Args:
        message (types.Message): Сообщение
        links (List[str]): Список VK ссылок
        user_id (int): ID пользователя
        download_msg (types.Message): Сообщение о скачивании файла
    """
    # Ограничиваем количество ссылок
    max_links = 500
    if len(links) > max_links:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=download_msg.message_id,
            text=f"⚠️ Обнаружено слишком много ссылок ({len(links)}). Будут обработаны первые {max_links}."
        )
        links = links[:max_links]

    # Обновляем активность пользователя
    db.update_user_activity(user_id)

    # Обновляем сообщение о статусе
    try:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=download_msg.message_id,
            text=f"🔍 Найдено {len(links)} ссылок на профили ВКонтакте.\n⚙️ Начинаю пакетную обработку...\n\n"
                 f"ℹ️ <b>Обратите внимание:</b> Обработка может занять несколько минут.",
            parse_mode="HTML"
        )
    except Exception as e:
        log_error(e, {"action": "updating download message"})
        processing_msg = await message.answer(
            f"🔍 Найдено {len(links)} ссылок на профили ВКонтакте.\n⚙️ Начинаю пакетную обработку...",
            parse_mode="HTML"
        )
        download_msg = processing_msg

    # Добавляем небольшую задержку перед обработкой
    await asyncio.sleep(0.5)

    start_time = time.time()

    try:
        # Обрабатываем ссылки с помощью функции process_vk_links
        results = await process_vk_links(
            links,
            user_id,
            message.chat.id,
            download_msg.message_id,
            bot,
            process_vk_search,
            db
        )

        # Рассчитываем затраченное время
        processing_time = time.time() - start_time
        time_str = f"{int(processing_time // 60)} мин {int(processing_time % 60)} сек"

        # Создаем файл с результатами
        result_file_path = create_results_file(results)

        # Считаем статистику
        results_with_phones = len([r for r in results if r[1] and len(r[1]) > 0])
        total_phones = sum(len(phones) for _, phones in results if phones)

        # Отправляем файл с результатами
        caption = (
            f"✅ Пакетная обработка завершена за {time_str}.\n"
            f"📊 Статистика:\n"
            f"- Обработано ссылок: {len(results)}\n"
            f"- Найдено ссылок с номерами: {results_with_phones}/{len(results)}\n"
            f"- Всего найдено номеров: {total_phones}\n\n"
            f"📄 Результаты сохранены в Excel файле (.xlsx)."
        )

        await bot.send_document(
            chat_id=message.chat.id,
            document=InputFile(result_file_path),
            caption=caption
        )

    except Exception as e:
        log_error(e, {"user_id": user_id, "action": "batch processing", "details": str(e)})
        await message.answer(
            f"❌ Ошибка при обработке файла: {str(e)}\n\n"
            f"Возможные причины:\n"
            f"- Проблемы с доступом к API\n"
            f"- Неверный формат ссылок\n"
            f"- Внутренняя ошибка сервера\n\n"
            f"Пожалуйста, попробуйте позже или обратитесь к администратору."
        )
    finally:
        # Возвращаем основную клавиатуру
        await message.answer("Готово! Чем еще я могу помочь?", reply_markup=get_main_keyboard())

async def send_phone_results(chat_id: int, phone: str, results: List[Dict[str, Any]], processing_msg_id: int = None):
    """
    Отправка результатов поиска по номеру телефона пользователю

    Args:
        chat_id (int): ID чата для отправки
        phone (str): Номер телефона
        results (List[Dict[str, Any]]): Результаты поиска
        processing_msg_id (int, optional): ID сообщения о начале обработки
    """
    if not results:
        no_results_message = "📵 <b>По данному номеру телефона ничего не найдено</b>\n\nПопробуйте другой номер или воспользуйтесь поиском по VK ID."

        if processing_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=processing_msg_id,
                    text=no_results_message,
                    parse_mode="HTML"
                )
            except (aiogram.utils.exceptions.MessageCantBeEdited,
                    aiogram.utils.exceptions.MessageNotModified,
                    aiogram.utils.exceptions.MessageToEditNotFound,
                    aiogram.utils.exceptions.CantParseEntities) as e:
                # Если не можем отредактировать, отправляем новое сообщение
                logger.warning(f"Could not edit message, sending new one: {e}")
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=no_results_message,
                        parse_mode="HTML"
                    )
                except aiogram.utils.exceptions.CantParseEntities:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=html.unescape(no_results_message).replace("<b>", "").replace("</b>", "")
                    )
        else:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=no_results_message,
                    parse_mode="HTML"
                )
            except aiogram.utils.exceptions.CantParseEntities:
                await bot.send_message(
                    chat_id=chat_id,
                    text=html.unescape(no_results_message).replace("<b>", "").replace("</b>", "")
                )
        return

    # Format message with HTML
    message_parts = []
    message_parts.append(f"<b>📱 Найденные телефонные номера ({len(results)} шт.)</b>\n")

    # Group by VK ID
    vk_ids = {}
    for result in results:
        vk_id = result.get("vk_id", "Неизвестно")
        if vk_id not in vk_ids:
            vk_ids[vk_id] = []
        vk_ids[vk_id].append(result)

    # Format results by VK ID
    for vk_id, vk_results in vk_ids.items():
        message_parts.append(f"\n<b>👤 VK ID: {html.escape(vk_id)}</b>")

        # Add name if available
        if vk_results[0].get("full_name"):
            message_parts.append(f"👤 Имя: {html.escape(vk_results[0]['full_name'])}")

        # Add source
        if vk_results[0].get("source"):
            message_parts.append(f"📊 Источник: {html.escape(vk_results[0]['source'])}")

    # Create keyboard for actions
    markup = InlineKeyboardMarkup(row_width=1)

    # Add buttons for quick search by VK ID
    for vk_id in list(vk_ids.keys())[:3]:
        if vk_id.isdigit() or (vk_id.startswith('id') and vk_id[2:].isdigit()):
            clean_id = vk_id[2:] if vk_id.startswith('id') else vk_id
            markup.add(InlineKeyboardButton(
                f"🔍 Искать информацию по VK ID: {vk_id}",
                callback_data=f"search_vk:{clean_id}"
            ))
            markup.add(InlineKeyboardButton(
                f"🌐 Открыть профиль VK: {vk_id}",
                url=f"https://vk.com/id{clean_id}"
            ))

    # Add timestamp
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    message_parts.append(f"\n<i>⏱ Отчет сгенерирован: {now}</i>")

    # Send message with results
    html_message = "\n".join(message_parts)

    if processing_msg_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=processing_msg_id,
                text=html_message,
                parse_mode="HTML",
                reply_markup=markup
            )
        except (aiogram.utils.exceptions.MessageCantBeEdited,
                aiogram.utils.exceptions.MessageNotModified,
                aiogram.utils.exceptions.MessageToEditNotFound,
                aiogram.utils.exceptions.CantParseEntities) as e:
            # Пробуем отправить новое сообщение
            logger.warning(f"Could not edit message, sending new one: {e}")
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=html_message,
                    parse_mode="HTML",
                    reply_markup=markup
                )
            except aiogram.utils.exceptions.CantParseEntities as html_error:
                # Если не получается использовать HTML, пробуем без форматирования
                logger.error(f"Error sending with HTML: {html_error}")
                plain_text = html.unescape(html_message).replace("<b>", "").replace("</b>", "").replace("<i>",
                                                                                                        "").replace(
                    "</i>", "")
                await bot.send_message(
                    chat_id=chat_id,
                    text=plain_text,
                    reply_markup=markup
                )
            except Exception as send_error:
                logger.error(f"Failed to send message after edit failed: {send_error}")
        except Exception as e:
            # Обрабатываем прочие ошибки
            logger.error(f"Error sending phone results: {e}")
            try:
                plain_text = html.unescape(html_message).replace("<b>", "").replace("</b>", "").replace("<i>",
                                                                                                        "").replace(
                    "</i>", "")
                await bot.send_message(
                    chat_id=chat_id,
                    text=plain_text,
                    reply_markup=markup
                )
            except Exception as send_error:
                logger.error(f"Failed to send message after edit failed: {send_error}")
    else:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=html_message,
                parse_mode="HTML",
                reply_markup=markup
            )
        except aiogram.utils.exceptions.CantParseEntities:
            plain_text = html.unescape(html_message).replace("<b>", "").replace("</b>", "").replace("<i>", "").replace(
                "</i>", "")
            await bot.send_message(
                chat_id=chat_id,
                text=plain_text,
                reply_markup=markup
            )
        except Exception as e:
            logger.error(f"Error sending phone results: {e}")
            try:
                plain_text = html.unescape(html_message).replace("<b>", "").replace("</b>", "").replace("<i>",
                                                                                                        "").replace(
                    "</i>", "")
                await bot.send_message(
                    chat_id=chat_id,
                    text=plain_text,
                    reply_markup=markup
                )
            except Exception as send_error:
                logger.error(f"Failed to send message: {send_error}")


def method_to_text(method, *args):
    """
    Преобразует метод поиска в читаемый текст

    Args:
        method (str): Метод поиска
        *args: Дополнительные аргументы

    Returns:
        str: Текстовое описание метода
    """
    methods = {
        "name_dob_search": "Поиск по ФИО и дате рождения",
        "email_search": "Поиск по email",
        "vk_search": "Поиск по VK ID",
        "unknown": "Комбинированный поиск"
    }
    return methods.get(method, "Неизвестный метод")


def _validate_vk_id(vk_id: str, *args) -> bool:
    """
    Проверка корректности формата VK ID

    Args:
        vk_id (str): VK ID для проверки
        *args: Дополнительные аргументы

    Returns:
        bool: True, если формат корректный, иначе False
    """
    if not vk_id:
        return False

    # Проверяем числовой формат
    if vk_id.isdigit():
        return True

    # Проверяем формат id123456
    if vk_id.startswith('id') and vk_id[2:].isdigit():
        return True

    return False


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """
    Создание основной клавиатуры бота

    Returns:
        ReplyKeyboardMarkup: Основная клавиатура
    """
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(KeyboardButton("🔍 Поиск по ФИО + ДР"), KeyboardButton("🔍 Поиск по VK ID"))
    keyboard.add(KeyboardButton("📱 Поиск по телефону"), KeyboardButton("📂 Обработка файла"))
    keyboard.add(KeyboardButton("⚙️ Настройки"), KeyboardButton("❓ Помощь"))
    return keyboard


def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    """
    Создание клавиатуры с кнопкой отмены

    Returns:
        ReplyKeyboardMarkup: Клавиатура с кнопкой отмены
    """
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(KeyboardButton("Отмена"))
    return keyboard


def create_pagination_keyboard(query_id: str, current_page: int, total_pages: int) -> InlineKeyboardMarkup:
    """
    Создание клавиатуры для пагинации

    Args:
        query_id (str): ID запроса
        current_page (int): Текущая страница
        total_pages (int): Общее количество страниц

    Returns:
        InlineKeyboardMarkup: Клавиатура для пагинации
    """
    markup = InlineKeyboardMarkup(row_width=3)  # Используем корректное создание с row_width
    row = []

    # Кнопка "Назад"
    if current_page > 0:
        row.append(InlineKeyboardButton(
            "◀️ Назад",
            callback_data=page_cb.new(query_id=query_id, page_id=current_page - 1)
        ))

    # Индикатор страниц
    row.append(InlineKeyboardButton(
        f"{current_page + 1} / {total_pages}",
        callback_data="current_page"
    ))

    # Кнопка "Вперед"
    if current_page < total_pages - 1:
        row.append(InlineKeyboardButton(
            "Вперед ▶️",
            callback_data=page_cb.new(query_id=query_id, page_id=current_page + 1)
        ))

    markup.row(*row)
    return markup


def clean_cache():
    """Очистка устаревших данных из кеша результатов"""
    current_time = time.time()
    to_delete = []

    for query_id in cached_results.keys():
        # Если запись старше 30 минут, помечаем для удаления
        if query_id.startswith('timestamp_'):
            timestamp = float(query_id.split('_')[1])
            if current_time - timestamp > CACHE_CLEANUP_INTERVAL:
                # Получаем связанный ID запроса
                related_id = query_id.split('_', 2)[2]
                to_delete.append(related_id)
                to_delete.append(query_id)

    # Удаляем устаревшие записи
    for key in to_delete:
        if key in cached_results:
            del cached_results[key]

    logger.info(f"Cleaned {len(to_delete)} cached results")


async def on_startup(dp):
    """Действия при запуске бота"""
    logger.info("Bot started")

    # Очистка старых файлов при запуске
    formatter.cleanup_old_files()
    formatter.cleanup_old_json_files()

    # Очистка устаревшего кеша
    db.clean_expired_cache()

    # Регистрация команд в меню бота
    await register_bot_commands(dp.bot)


async def register_bot_commands(bot_instance):
    """Регистрация команд для отображения в меню бота"""
    commands = [
        types.BotCommand("start", "Запустить бота"),
        types.BotCommand("help", "Показать справку"),
        types.BotCommand("search", "Поиск по ФИО + дате рождения"),
        types.BotCommand("vk", "Поиск по VK ID"),
        types.BotCommand("phone", "Поиск по номеру телефона"),
        types.BotCommand("process_file", "Обработка файла со списком VK профилей"),
        types.BotCommand("process_name_dob", "Обработка файла со списком ФИО + ДР"),
        types.BotCommand("api_status", "Проверить доступность API сервера"),
        types.BotCommand("settings", "Настройки бота"),
        types.BotCommand("cancel", "Отменить текущую операцию")
    ]

    try:
        await bot_instance.set_my_commands(commands)
        logger.info("Bot commands have been registered successfully")
    except Exception as e:
        logger.error(f"Error registering bot commands: {e}")


async def scheduled_jobs():
    """Задачи по расписанию"""
    # Очистка устаревшего кеша
    deleted_count = db.clean_expired_cache()
    logger.info(f"Scheduled job: Cleaned {deleted_count} expired cache entries")

    # Очистка старых временных файлов
    formatter.cleanup_old_files()
    logger.info("Scheduled job: Cleaned old temporary files")

    # Очистка старых JSON файлов
    formatter.cleanup_old_json_files()
    logger.info("Scheduled job: Cleaned old JSON files")

    # Очистка кеша результатов
    clean_cache()
    logger.info("Scheduled job: Cleaned old cached results")


# Функция для планировщика
def schedule_loop():
    """Цикл для выполнения запланированных задач"""
    shutdown_event = threading.Event()

    while not shutdown_event.is_set():
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f"Error in scheduler: {e}")
        time.sleep(60)  # Проверка каждую минуту

    logger.info("Scheduler thread stopped")

    return shutdown_event  # Возвращаем событие для возможности завершения потока


async def advanced_search(query: str, user_id: int, user_settings: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Выполняет комплексный поиск по ФИО и дате рождения с последующим поиском по email

    Args:
        query (str): Запрос в формате "Фамилия Имя ДД.ММ.ГГГГ"
        user_id (int): ID пользователя
        user_settings (Dict[str, Any]): Настройки пользователя

    Returns:
        Dict[str, Any]: Результаты поиска с дополнительной информацией
    """
    import time
    import re
    from file_processing import analyze_first_stage_results, analyze_second_stage_results, extract_phone_from_vk_parsing, extract_phones_from_api_response

    if not user_settings:
        user_settings = db.get_user_settings(user_id)

    # Результаты поиска
    results = {
        "query": query,
        "phones": [],
        "method": None,
        "confidence": 0.0,
        "stages_info": []
    }

    # Подготовка стандартизированного запроса
    parts = query.split()
    dob_pattern = re.compile(r'^\d{1,2}[./-]\d{1,2}[./-]\d{2,4}$')

    # Анализ запроса
    date_of_birth = ""
    surname = ""
    name = ""

    if len(parts) >= 3 and dob_pattern.match(parts[-1]):
        date_of_birth = parts[-1]
        name_parts = parts[:-1]
        surname = name_parts[0].lower()
        name = " ".join(name_parts[1:]).lower()

        # Стандартизация даты рождения в формат YYYY-MM-DD
        if re.match(r'^\d{2}\.\d{2}\.\d{4}$', date_of_birth):
            dob_parts = date_of_birth.split('.')
            iso_date = f"{dob_parts[2]}-{dob_parts[1]}-{dob_parts[0]}"
            standardized_date = iso_date
        else:
            standardized_date = date_of_birth

    logger.info(f"Используется улучшенный алгоритм поиска для запроса: {query}")
    logger.info(f"Запрос поиска по ФИО и дате рождения: {surname} {name}, {date_of_birth}")

    data = {
        "surname": surname,
        "first_name": name,
        "patronymic": "",
        "full_name": f"{surname} {name}".strip()
    }

    logger.info(f"Стандартизированные данные для поиска: {data}, дата: {standardized_date}")

    # Фоновый запуск поиска (для асинхронности)
    loop = asyncio.get_event_loop()
    start_time = time.time()

    # Формируем основной запрос
    main_query = f"{data['surname']} {data['first_name']} {standardized_date}".strip()
    logger.info(f"Отправка основного запроса: {main_query}")

    # Очищаем кеш для этого запроса, чтобы получить свежие данные
    cache_key = f"name_dob:{main_query}:{user_settings.get('language', 'ru')}:{user_settings.get('limit', 300)}"
    try:
        # Проверяем наличие кеша
        cached_response = db.get_cached_response(cache_key)
        if cached_response:
            logger.info(f"Найден кешированный ответ для запроса: {main_query}")
            # Удаляем кеш, если есть ключевые слова для принудительного запроса
            if "force_refresh" in query.lower() or "обновить" in query.lower():
                db.delete_cached_response(cache_key)
                logger.info(f"Удален кешированный ответ для принудительного обновления")
                cached_response = None
    except Exception as e:
        logger.error(f"Ошибка при проверке кеша: {e}")
        cached_response = None

    # ЭТАП 1: Поиск по ФИО и дате рождения
    try:
        # Выполняем запрос к API
        first_stage_response = await loop.run_in_executor(
            None,
            lambda: api_client.search_by_name_dob(
                main_query,
                lang=user_settings.get("language", "ru"),
                limit=user_settings.get("limit", 300)
            )
        )

        # Сохраняем информацию о запросе
        results["stages_info"].append({
            "stage": "first_query",
            "query": main_query,
            "status": "completed"
        })

        # ВАЖНОЕ ИЗМЕНЕНИЕ: Прямой поиск телефонов в ответе
        direct_phones = extract_phones_from_api_response(first_stage_response)
        if direct_phones:
            logger.info(f"Найдено {len(direct_phones)} телефонов прямым методом")
            results["phones"] = direct_phones
            results["confidence"] = 0.9  # Высокая уверенность для прямого метода
            results["method"] = "direct_extract"
            return results

        # Продолжаем стандартный анализ результатов
        emails, phones, confidence, vk_ids = analyze_first_stage_results(first_stage_response, query)

        # Сохраняем результаты первого этапа
        results["phones"].extend(phones)
        results["confidence"] = confidence

        logger.info(
            f"Результаты основного запроса: {len(phones)} телефонов, {len(emails)} email, уверенность {confidence:.2f}")

        # Если основной запрос не дал результатов, пробуем вариации
        if not phones and not emails:
            logger.info(f"Основной запрос не дал результатов, пробуем запросы с частичным совпадением")

            # Вариант 1: Только фамилия + дата
            partial_query1 = f"{data['surname']} {standardized_date}".strip()
            logger.info(f"Отправка запроса с частичным совпадением 1: {partial_query1}")

            # Проверяем кеш для частичного запроса
            partial_cache_key1 = f"name_dob:{partial_query1}:{user_settings.get('language', 'ru')}:{user_settings.get('limit', 300)}"
            try:
                db.delete_cached_response(partial_cache_key1)
            except:
                pass

            partial_response1 = await loop.run_in_executor(
                None,
                lambda: api_client.search_by_name_dob(
                    partial_query1,
                    lang=user_settings.get("language", "ru"),
                    limit=user_settings.get("limit", 300)
                )
            )

            # Прямой поиск телефонов в частичном ответе
            direct_phones1 = extract_phones_from_api_response(partial_response1)
            if direct_phones1:
                logger.info(f"Найдено {len(direct_phones1)} телефонов прямым методом в частичном запросе 1")
                results["phones"] = direct_phones1
                results["confidence"] = 0.85  # Немного ниже уверенность для частичного запроса
                results["method"] = "direct_extract_partial1"
                return results

            # Стандартный анализ результатов частичного запроса
            partial_emails1, partial_phones1, partial_confidence1, partial_vk_ids1 = analyze_first_stage_results(
                partial_response1, partial_query1)

            if partial_phones1 or partial_emails1:
                logger.info(
                    f"Найдены результаты в частичном запросе 1: {len(partial_phones1)} телефонов, {len(partial_emails1)} email")
                emails.extend([e for e in partial_emails1 if e not in emails])
                phones.extend([p for p in partial_phones1 if p not in phones])
                vk_ids.extend([v for v in partial_vk_ids1 if v not in vk_ids])
                confidence = max(confidence, partial_confidence1)

            # Вариант 2: Только имя + дата, если не нашли в первом варианте
            if not direct_phones1 and not partial_phones1 and not partial_emails1 and data['first_name']:
                partial_query2 = f"{data['first_name']} {standardized_date}".strip()
                logger.info(f"Отправка запроса с частичным совпадением 2: {partial_query2}")

                # Проверяем кеш для частичного запроса
                partial_cache_key2 = f"name_dob:{partial_query2}:{user_settings.get('language', 'ru')}:{user_settings.get('limit', 300)}"
                try:
                    db.delete_cached_response(partial_cache_key2)
                except:
                    pass

                partial_response2 = await loop.run_in_executor(
                    None,
                    lambda: api_client.search_by_name_dob(
                        partial_query2,
                        lang=user_settings.get("language", "ru"),
                        limit=user_settings.get("limit", 300)
                    )
                )

                # Прямой поиск телефонов в частичном ответе 2
                direct_phones2 = extract_phones_from_api_response(partial_response2)
                if direct_phones2:
                    logger.info(f"Найдено {len(direct_phones2)} телефонов прямым методом в частичном запросе 2")
                    results["phones"] = direct_phones2
                    results["confidence"] = 0.8  # Еще ниже уверенность
                    results["method"] = "direct_extract_partial2"
                    return results

                # Стандартный анализ
                partial_emails2, partial_phones2, partial_confidence2, partial_vk_ids2 = analyze_first_stage_results(
                    partial_response2, partial_query2)

                if partial_emails2 or partial_phones2:
                    logger.info(
                        f"Найдены результаты в частичном запросе 2: {len(partial_phones2)} телефонов, {len(partial_emails2)} email")
                    emails.extend([e for e in partial_emails2 if e not in emails])
                    phones.extend([p for p in partial_phones2 if p not in phones])
                    vk_ids.extend([v for v in partial_vk_ids2 if v not in vk_ids])
                    confidence = max(confidence, partial_confidence2)

        # Обновляем результаты с учетом всех запросов
        results["phones"] = phones
        results["confidence"] = confidence

        # Если найдены телефоны с высокой уверенностью, возвращаем результат
        if phones and confidence >= 0.7:
            results["method"] = "name_dob_search"
            return results

        # ЭТАП 2: Поиск по email (если найден и нет телефонов)
        if emails and (not phones or confidence < 0.7):
            logger.info(f"ЭТАП 2: Поиск по email: {emails[0]}")

            # Очищаем кеш для email-запроса
            email_cache_key = f"email:{emails[0]}:{user_settings.get('language', 'ru')}:{user_settings.get('limit', 300)}"
            try:
                db.delete_cached_response(email_cache_key)
            except:
                pass

            # Выполняем запрос к API по первому найденному email
            second_stage_response = await loop.run_in_executor(
                None,
                lambda: api_client.make_request(
                    query=emails[0],
                    lang=user_settings.get("language", "ru"),
                    limit=2000,  # Увеличиваем лимит для email-запроса
                    result_type="json"
                )
            )

            # Сохраняем информацию о запросе
            results["stages_info"].append({
                "stage": "email_query",
                "query": emails[0],
                "status": "completed"
            })

            # ВАЖНОЕ ИЗМЕНЕНИЕ: Прямой поиск телефонов в ответе на email-запрос
            email_phones = extract_phones_from_api_response(second_stage_response)
            if email_phones:
                logger.info(f"Найдено {len(email_phones)} телефонов прямым методом в email-запросе")
                results["phones"] = email_phones
                results["confidence"] = 0.85  # Хорошая уверенность для email-запроса
                results["method"] = "email_direct_extract"
                return results

            # Стандартный анализ результатов
            response_str = str(second_stage_response)
            logger.debug(f"Полный ответ на запрос по email (размер строки): {len(response_str)}")

            # Проверка на пометку "Номер который нужно забирать"
            if "Номер который нужно забирать" in response_str:
                logger.info(f"Найдена пометка 'Номер который нужно забирать' в ответе по email")
                phone_pattern = re.compile(r'📞Телефон:\s*(\d+)[^)]*Номер который нужно забирать')
                phone_matches = phone_pattern.findall(response_str)

                for phone in phone_matches:
                    if phone.startswith('79') and len(phone) == 11 and phone not in results["phones"]:
                        results["phones"].insert(0, phone)  # Добавляем в начало как приоритетный
                        results["confidence"] = 0.95  # Высокая уверенность
                        results["method"] = "email_search_marked"
                        logger.info(f"Найден телефон с пометкой в email-запросе: {phone}")

            # Поиск любых телефонов в ответе по email
            phone_pattern = re.compile(r'📞Телефон:\s*(\d+)')
            phone_matches = phone_pattern.findall(response_str)

            for phone in phone_matches:
                if phone.startswith('79') and len(phone) == 11 and phone not in results["phones"]:
                    results["phones"].append(phone)
                    if results["confidence"] < 0.8:
                        results["confidence"] = 0.8  # Хорошая уверенность
                        results["method"] = "email_search"
                    logger.info(f"Найден телефон в email-запросе: {phone}")

            # Если нашли телефоны, возвращаем результат
            if results["phones"]:
                return results

        # ЭТАП 3: Поиск по VK ID (если найден)
        if vk_ids and not results["phones"]:
            logger.info(f"ЭТАП 3: Поиск по VK ID: {vk_ids[0]}")

            # Очищаем кеш для VK ID-запроса
            vk_cache_key = f"vk:{vk_ids[0]}:{user_settings.get('language', 'ru')}:{user_settings.get('limit', 300)}"
            try:
                db.delete_cached_response(vk_cache_key)
            except:
                pass

            # Используем существующую функцию поиска по VK ID
            vk_response = await process_vk_search(vk_ids[0], user_id, user_settings)

            # Сохраняем информацию о запросе
            results["stages_info"].append({
                "stage": "vk_id_query",
                "query": vk_ids[0],
                "status": "completed"
            })

            # ВАЖНОЕ ИЗМЕНЕНИЕ: Прямой поиск телефонов в ответе на VK ID-запрос
            vk_direct_phones = extract_phones_from_api_response(vk_response)
            if vk_direct_phones:
                logger.info(f"Найдено {len(vk_direct_phones)} телефонов прямым методом в VK ID-запросе")
                results["phones"] = vk_direct_phones
                results["confidence"] = 0.8  # Хорошая уверенность для VK ID
                results["method"] = "vk_direct_extract"
                return results

            # Извлекаем телефоны стандартным методом
            vk_phones = extract_phone_from_vk_parsing(vk_response, vk_ids[0])

            # Добавляем новые телефоны (без дубликатов)
            for phone in vk_phones:
                if phone not in results["phones"]:
                    results["phones"].append(phone)

            if vk_phones:
                results["method"] = "vk_search"
                results["confidence"] = 0.7  # Хорошая уверенность для VK ID
                return results

    except Exception as e:
        logger.error(f"Ошибка при выполнении комплексного поиска: {e}")
        logger.error(traceback.format_exc())
        return {
            "query": query,
            "phones": [],
            "method": None,
            "confidence": 0.0,
            "error": str(e)
        }

    # Возвращаем все собранные результаты
    return results


if __name__ == "__main__":
    try:
        # Проверяем состояние базы данных перед запуском
        logger.info("Проверка состояния базы данных...")
        if not db.check_database_health():
            logger.warning("База данных не в порядке, выполняется восстановление таблиц")
            # Принудительно пересоздаем все таблицы
            with db.get_connection() as conn:
                db.create_tables(conn)
            # Повторная проверка после восстановления
            if not db.check_database_health():
                logger.critical("Невозможно восстановить базу данных! Завершение программы.")
                sys.exit(1)

        # Запускаем фоновые задачи по расписанию
        # Очистка кеша и временных файлов каждый день в 03:00
        schedule.every().day.at("03:00").do(lambda: asyncio.run(scheduled_jobs()))
        # Очистка кеша результатов каждые 30 минут
        schedule.every(30).minutes.do(clean_cache)

        # Запуск планировщика в отдельном потоке
        scheduler_thread = threading.Thread(target=schedule_loop)
        scheduler_thread.daemon = True
        scheduler_thread.start()

        logger.info("Starting bot...")
        # Запуск бота
        executor.start_polling(dp, on_startup=on_startup, skip_updates=True)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}")
        logger.critical(traceback.format_exc())