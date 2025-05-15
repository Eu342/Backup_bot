from config.settings import telegram_bot, dp, ADMIN_LIST, logger, DUMPS_DIR, ERROR_DUMPS_DIR
from bot.states import DeployStates
from aiogram import types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pathlib import Path
from deploy.deploy import deploy_dump
from backups.utils import read_file_lines, unlink_file, async_archive_dump, run_subprocess
from backups.manager import create_backup_now
import zipfile
import os
import asyncio

# Диагностика
logger.debug(f"Инициализация handlers.py, dp id: {id(dp)}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработка команды /start."""
    logger.debug(f"Получена команда /start от пользователя {message.from_user.id}")
    await message.reply(f"Бот работает! Ваш ID: {message.from_user.id}")

@dp.message(Command("backup_deploy"))
async def cmd_backup_deploy(message: types.Message, state: FSMContext):
    """Обработка команды /backup_deploy."""
    logger.debug(f"Получена команда /backup_deploy от пользователя {message.from_user.id}")
    if str(message.from_user.id) not in ADMIN_LIST:
        logger.warning(f"Несанкционированная попытка /backup_deploy от пользователя {message.from_user.id}")
        await message.reply("Доступ запрещён: вы не админ.")
        return
    
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        sent_message = await message.reply(
            "Отправьте файл дампа (.sql или .zip) или укажите его название (например, opengater_test_20250514_XXXXXX.sql или opengater_test_20250514_XXXXXX.zip).",
            reply_markup=keyboard
        )
        await state.update_data(current_message_id=sent_message.message_id, chat_id=sent_message.chat.id)
        await state.set_state(DeployStates.waiting_for_dump)
        logger.debug(f"Установлено состояние DeployStates.waiting_for_dump для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка в cmd_backup_deploy: {e}")
        await message.reply(f"Ошибка: {e}")

@dp.message(Command("backup_create"))
async def cmd_backup_create(message: types.Message):
    """Обработка команды /backup_create."""
    logger.debug(f"Получена команда /backup_create от пользователя {message.from_user.id}")
    if str(message.from_user.id) not in ADMIN_LIST:
        logger.warning(f"Несанкционированная попытка /backup_create от пользователя {message.from_user.id}")
        await message.reply("Доступ запрещён: вы не админ.")
        return
    
    try:
        await message.reply("Запуск создания бэкапа...")
        results = await create_backup_now()
        
        if not results:
            logger.warning("Бэкапы не созданы: результаты пусты")
            await message.reply("Бэкапы не созданы. Проверьте логи для ошибок.")
            return
        
        response = "Создание бэкапов завершено:\n"
        for result in results:
            response += f"- База: {result['database']}\n  Архив: {result['archive']}\n"
            if result['download_url']:
                response += f"  URL для скачивания: {result['download_url']}\n"
            else:
                response += "  Не удалось загрузить на файлообменник\n"
        
        await message.reply(response)
        logger.debug(f"Успешно отправлен ответ для /backup_create: {response}")
    except Exception as e:
        logger.error(f"Ошибка в cmd_backup_create: {e}")
        await message.reply(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "cancel_deploy")
async def cancel_deploy(callback: types.CallbackQuery, state: FSMContext):
    """Обработка отмены процесса развёртывания."""
    logger.debug(f"Получен callback cancel_deploy от пользователя {callback.from_user.id}")
    try:
        await callback.message.delete()
        await callback.message.answer("Процесс развёртывания отменён.")
    except Exception as e:
        logger.error(f"Не удалось удалить сообщение при отмене: {e}")
        await callback.message.edit_text("Процесс развёртывания отменён.")
    await state.clear()
    await callback.answer()

@dp.message(DeployStates.waiting_for_dump)
async def process_dump(message: types.Message, state: FSMContext):
    """Обработка дампа (файл или название)."""
    logger.debug(f"Получен дамп от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    dump_path = None
    temp_file = None
    temp_zip = None
    try:
        if message.document:
            logger.debug("Получен файл дампа через Telegram")
            file = await telegram_bot.get_file(message.document.file_id)
            file_path = file.file_path
            file_name = message.document.file_name
            if not file_name.endswith(('.sql', '.zip')):
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                ])
                await telegram_bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=current_message_id,
                    text="Неподдерживаемый формат файла. Отправьте .sql или .zip.",
                    reply_markup=keyboard
                )
                logger.warning(f"Получен неподдерживаемый файл: {file_name}")
                return
            
            temp_file = DUMPS_DIR / file_name
            await telegram_bot.download_file(file_path, temp_file)
            logger.debug(f"Скачан файл дампа: {temp_file}")
            
            if file_name.endswith('.zip'):
                with zipfile.ZipFile(temp_file, 'r') as zf:
                    sql_files = [f for f in zf.namelist() if f.endswith('.sql')]
                    if not sql_files:
                        keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                        ])
                        await telegram_bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=current_message_id,
                            text="ZIP-архив не содержит .sql файлов.",
                            reply_markup=keyboard
                        )
                        await unlink_file(temp_file)
                        logger.error(f"ZIP-архив {temp_file} не содержит .sql файлов")
                        return
                    if len(sql_files) > 1:
                        keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                        ])
                        await telegram_bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=current_message_id,
                            text="ZIP-архив содержит несколько .sql файлов. Отправьте архив с одним файлом.",
                            reply_markup=keyboard
                        )
                        await unlink_file(temp_file)
                        logger.error(f"ZIP-архив {temp_file} содержит несколько .sql файлов")
                        return
                    zf.extract(sql_files[0], DUMPS_DIR)
                    dump_path = DUMPS_DIR / sql_files[0]
                await unlink_file(temp_file)
                logger.debug(f"Распакован ZIP, дамп: {dump_path}")
            else:
                dump_path = temp_file
        else:
            file_name = message.text.strip()
            logger.debug(f"Получено имя дампа: {file_name}")
            dump_path = None
            for db_dir in DUMPS_DIR.iterdir():
                if db_dir.is_dir():
                    zip_path = db_dir / file_name
                    if zip_path.exists() and zip_path.suffix == '.zip':
                        temp_zip = zip_path
                        with zipfile.ZipFile(temp_zip, 'r') as zf:
                            sql_files = [f for f in zf.namelist() if f.endswith('.sql')]
                            if not sql_files:
                                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                                ])
                                await telegram_bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=current_message_id,
                                    text=f"ZIP-архив {file_name} не содержит .sql файлов.",
                                    reply_markup=keyboard
                                )
                                logger.error(f"ZIP-архив {file_name} не содержит .sql файлов")
                                return
                            if len(sql_files) > 1:
                                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                                ])
                                await telegram_bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=current_message_id,
                                    text=f"ZIP-архив {file_name} содержит несколько .sql файлов. Укажите архив с одним файлом.",
                                    reply_markup=keyboard
                                )
                                logger.error(f"ZIP-архив {file_name} содержит несколько .sql файлов")
                                return
                            zf.extract(sql_files[0], DUMPS_DIR)
                            dump_path = DUMPS_DIR / sql_files[0]
                        logger.debug(f"Распакован указанный ZIP-архив: {dump_path}")
                        break
                    sql_path = db_dir / file_name
                    if sql_path.exists() and sql_path.suffix == '.sql':
                        dump_path = sql_path
                        logger.debug(f"Найден указанный .sql дамп: {dump_path}")
                        break
                    if not file_name.endswith(('.sql', '.zip')):
                        sql_path = db_dir / f"{file_name}.sql"
                        if sql_path.exists():
                            dump_path = sql_path
                            logger.debug(f"Найден указанный .sql дамп: {dump_path}")
                            break
            
            if not dump_path or not dump_path.exists():
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                ])
                await telegram_bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=current_message_id,
                    text=f"Дамп {file_name} не найден в {DUMPS_DIR}. Укажите существующий .sql или .zip файл.",
                    reply_markup=keyboard
                )
                logger.error(f"Дамп {file_name} не найден в {DUMPS_DIR}")
                return
        
        full_content = await asyncio.to_thread(lambda: open(dump_path, 'r', encoding='utf-8').read())
        if not any(keyword in full_content.lower() for keyword in ['create table', 'insert into']):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text="Дамп пуст или не содержит таблиц/данных.",
                reply_markup=keyboard
            )
            logger.error(f"Дамп {dump_path} пуст или не содержит таблиц/данных")
            if temp_file and temp_file.exists():
                await unlink_file(temp_file)
            if dump_path and dump_path.exists() and dump_path != temp_file:
                await unlink_file(dump_path)
            return
        
        first_lines = await read_file_lines(dump_path, num_lines=100)
        logger.debug(f"Первые строки дампа {dump_path}:\n{first_lines[:200]}")
        mysql_keywords = ['/*!40101 set', '-- mysql dump', 'engine=innodb', 'lock tables']
        postgresql_keywords = ['create schema', 'set search_path', 'create sequence', 'copy public.']
        found_mysql = [kw for kw in mysql_keywords if kw in first_lines.lower()]
        found_postgresql = [kw for kw in postgresql_keywords if kw in first_lines.lower()]
        logger.debug(f"Найдены ключевые слова MySQL: {found_mysql}")
        logger.debug(f"Найдены ключевые слова PostgreSQL: {found_postgresql}")
        if found_mysql:
            db_type = 'mysql'
        elif found_postgresql:
            db_type = 'postgresql'
        else:
            db_type = 'postgresql'
            logger.warning(f"Тип дампа не определён, используется по умолчанию: postgresql")
        logger.debug(f"Определён тип дампа: {db_type}")
        
        await state.update_data(
            dump_path=str(dump_path),
            db_type=db_type,
            temp_file=temp_file,
            temp_zip=temp_zip,
            dump_message="Отправьте файл дампа (.sql или .zip) или укажите его название (например, opengater_test_20250514_XXXXXX.sql или opengater_test_20250514_XXXXXX.zip)."
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_dump")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Дамп принят ({'PostgreSQL' if db_type == 'postgresql' else 'MySQL'}). Укажите IP удалённого сервера.",
            reply_markup=keyboard
        )
        await state.set_state(DeployStates.waiting_for_ip)
        logger.debug(f"Установлено состояние DeployStates.waiting_for_ip для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка обработки дампа: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка обработки дампа: {e}",
            reply_markup=keyboard
        )
        if temp_file and temp_file.exists():
            await unlink_file(temp_file)
        if dump_path and dump_path.exists() and dump_path != temp_file:
            await unlink_file(dump_path)
        await state.clear()

@dp.callback_query(lambda c: c.data == "back_to_dump")
async def back_to_dump(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к шагу выбора дампа."""
    logger.debug(f"Получен callback back_to_dump от пользователя {callback.from_user.id}")
    data = await state.get_data()
    dump_message = data.get('dump_message', "Отправьте файл дампа (.sql или .zip) или укажите его название (например, opengater_test_20250514_XXXXXX.sql или opengater_test_20250514_XXXXXX.zip).")
    try:
        await callback.message.edit_text(
            dump_message,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await state.set_state(DeployStates.waiting_for_dump)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в back_to_dump: {e}")
        await callback.message.edit_text(f"Ошибка: {e}")

@dp.message(DeployStates.waiting_for_ip)
async def process_ip(message: types.Message, state: FSMContext):
    """Обработка IP удалённого сервера."""
    logger.debug(f"Получен IP от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    ip = message.text.strip()
    try:
        parts = ip.split('.')
        if len(parts) != 4 or not all(part.isdigit() and 0 <= int(part) <= 255 for part in parts):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_dump")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text="Некорректный IP-адрес. Укажите в формате X.X.X.X.",
                reply_markup=keyboard
            )
            logger.warning(f"Некорректный IP: {ip}")
            return
        await state.update_data(ip=ip, ip_message="Укажите IP удалённого сервера.")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_ip")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text="Укажите порт базы данных (например, 3306 для MySQL, 5432 для PostgreSQL).",
            reply_markup=keyboard
        )
        await state.set_state(DeployStates.waiting_for_port)
        logger.debug(f"Установлено состояние DeployStates.waiting_for_port для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка обработки IP: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_dump")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка обработки IP: {e}",
            reply_markup=keyboard
        )
        await state.clear()

@dp.callback_query(lambda c: c.data == "back_to_ip")
async def back_to_ip(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к шагу ввода IP."""
    logger.debug(f"Получен callback back_to_ip от пользователя {callback.from_user.id}")
    data = await state.get_data()
    ip_message = data.get('ip_message', "Укажите IP удалённого сервера.")
    try:
        await callback.message.edit_text(
            ip_message,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_dump")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await state.set_state(DeployStates.waiting_for_ip)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в back_to_ip: {e}")
        await callback.message.edit_text(f"Ошибка: {e}")

@dp.message(DeployStates.waiting_for_port)
async def process_port(message: types.Message, state: FSMContext):
    """Обработка порта базы данных."""
    logger.debug(f"Получен порт от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    port = message.text.strip()
    try:
        if not port.isdigit() or not (1 <= int(port) <= 65535):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_ip")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text="Некорректный порт. Укажите число от 1 до 65535.",
                reply_markup=keyboard
            )
            logger.warning(f"Некорректный порт: {port}")
            return
        await state.update_data(port=port, port_message="Укажите порт базы данных (например, 3306 для MySQL, 5432 для PostgreSQL).")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_port")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text="Укажите название базы данных.",
            reply_markup=keyboard
        )
        await state.set_state(DeployStates.waiting_for_dbname)
        logger.debug(f"Установлено состояние DeployStates.waiting_for_dbname для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка обработки порта: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_ip")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка обработки порта: {e}",
            reply_markup=keyboard
        )

@dp.callback_query(lambda c: c.data == "back_to_port")
async def back_to_port(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к шагу ввода порта."""
    logger.debug(f"Получен callback back_to_port от пользователя {callback.from_user.id}")
    data = await state.get_data()
    port_message = data.get('port_message', "Укажите порт базы данных (например, 3306 для MySQL, 5432 для PostgreSQL).")
    try:
        await callback.message.edit_text(
            port_message,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_ip")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await state.set_state(DeployStates.waiting_for_port)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в back_to_port: {e}")
        await callback.message.edit_text(f"Ошибка: {e}")

@dp.message(DeployStates.waiting_for_dbname)
async def process_dbname(message: types.Message, state: FSMContext):
    """Обработка названия базы данных."""
    logger.debug(f"Получено название базы данных от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    dbname = message.text.strip()
    try:
        if not dbname:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_port")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text="Название базы данных не может быть пустым.",
                reply_markup=keyboard
            )
            logger.warning("Пустое название базы данных")
            return

        await state.update_data(dbname=dbname, dbname_message="Укажите название базы данных.")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_port")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text="Укажите имя пользователя базы данных (рекомендуется использовать суперпользователя, например, postgres).",
            reply_markup=keyboard
        )
        await state.set_state(DeployStates.waiting_for_username)
        logger.debug(f"Установлено состояние DeployStates.waiting_for_username для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка обработки имени базы данных: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_port")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка обработки имени базы данных: {e}",
            reply_markup=keyboard
        )

@dp.callback_query(lambda c: c.data == "confirm_overwrite")
async def confirm_overwrite(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение перезаписи существующей базы."""
    logger.debug(f"Получен callback confirm_overwrite от пользователя {callback.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    dbname = data.get('dbname')
    dump_path = Path(data['dump_path'])
    db_type = data['db_type']
    ip = data['ip']
    port = data['port']
    username = data['username']
    password = data['password']
    temp_file = data.get('temp_file')
    temp_zip = data.get('temp_zip')

    try:
        await callback.answer("Развёртывание начато...")
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text="🔄 Развёртывание дампа начато, ожидайте...",
            reply_markup=None
        )

        logger.debug(f"Используется пользователь {username} для деплоймента")
        success, error = await deploy_dump(
            dump_path, db_type, ip, port, dbname, password, username,
            overwrite_confirmed=True, chat_id=chat_id, progress_message_id=current_message_id
        )
        if success:
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text=f"✅ Дамп успешно развёрнут на {ip}:{port}/{dbname}."
            )
            logger.info(f"Деплоймент дампа успешен: {dump_path} на {ip}:{port}/{dbname}")
            if temp_file and temp_file.exists():
                await unlink_file(temp_file)
                logger.debug(f"Удалён временный файл: {temp_file}")
            if dump_path.exists() and (not temp_file or dump_path != temp_file):
                await unlink_file(dump_path)
                logger.debug(f"Удалён временный дамп: {dump_path}")
            if temp_zip and temp_zip.exists():
                logger.debug(f"ZIP-архив {temp_zip} оставлен в хранилище")
            await state.clear()
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Повторить", callback_data="retry_password")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text=f"❌ Ошибка при развёртывании дампа: {error}",
                reply_markup=keyboard
            )
            logger.error(f"Ошибка деплоймента: {error}")
            logger.debug(f"Дамп сохранён для анализа в {ERROR_DUMPS_DIR}/{dump_path.name}")
    except Exception as e:
        logger.error(f"Ошибка при подтверждении перезаписи: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Повторить", callback_data="retry_password")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"❌ Ошибка: {e}",
            reply_markup=keyboard
        )
        if temp_file and temp_file.exists():
            await unlink_file(temp_file)
        logger.debug(f"Дамп сохранён для анализа в {ERROR_DUMPS_DIR}/{dump_path.name}")

@dp.callback_query(lambda c: c.data == "cancel_overwrite")
async def cancel_overwrite(callback: types.CallbackQuery, state: FSMContext):
    """Отмена перезаписи, возврат к вводу имени базы."""
    logger.debug(f"Получен callback cancel_overwrite от пользователя {callback.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    dbname_message = data.get('dbname_message', "Укажите название базы данных.")
    try:
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=dbname_message,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_port")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await state.set_state(DeployStates.waiting_for_dbname)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка при отмене перезаписи: {e}")
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка: {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await callback.answer()

@dp.message(DeployStates.waiting_for_username)
async def process_username(message: types.Message, state: FSMContext):
    """Обработка имени пользователя базы данных."""
    logger.debug(f"Получено имя пользователя от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    username = message.text.strip()
    try:
        if not username:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_dbname")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text="Имя пользователя не может быть пустым.",
                reply_markup=keyboard
            )
            logger.warning("Пустое имя пользователя")
            return
        await state.update_data(username=username, username_message="Укажите имя пользователя базы данных (рекомендуется использовать суперпользователя, например, postgres).")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_username")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text="Укажите пароль для подключения к базе данных.",
            reply_markup=keyboard
        )
        await state.set_state(DeployStates.waiting_for_password)
        logger.debug(f"Установлено состояние DeployStates.waiting_for_password для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка обработки имени пользователя: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_dbname")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка обработки имени пользователя: {e}",
            reply_markup=keyboard
        )

@dp.callback_query(lambda c: c.data == "back_to_username")
async def back_to_username(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к шагу ввода имени пользователя."""
    logger.debug(f"Получен callback back_to_username от пользователя {callback.from_user.id}")
    data = await state.get_data()
    username_message = data.get('username_message', "Укажите имя пользователя базы данных (рекомендуется использовать суперпользователя, например, postgres).")
    try:
        await callback.message.edit_text(
            username_message,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_dbname")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await state.set_state(DeployStates.waiting_for_username)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в back_to_username: {e}")
        await callback.message.edit_text(f"Ошибка: {e}")

@dp.message(DeployStates.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    """Обработка пароля и проверка базы."""
    logger.debug(f"Получен пароль от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    password = message.text.strip()
    dump_path = Path(data['dump_path'])
    db_type = data['db_type']
    ip = data['ip']
    port = data['port']
    dbname = data.get('dbname')
    username = data.get('username')
    temp_file = data.get('temp_file')
    temp_zip = data.get('temp_zip')

    try:
        env = os.environ.copy()
        env['PGPASSWORD' if db_type == 'postgresql' else 'MYSQL_PWD'] = password
        check_db_cmd = [
            'psql' if db_type == 'postgresql' else 'mysql',
            '-h', ip,
            '-P' if db_type == 'mysql' else '-p', port,
            '-u' if db_type == 'mysql' else '-U', username,
            '-d' if db_type == 'postgresql' else '-D', 'postgres' if db_type == 'postgresql' else dbname,
            '-t' if db_type == 'postgresql' else '--batch',
            '-c' if db_type == 'postgresql' else '-e',
            f"SELECT 1 FROM pg_database WHERE datname = '{dbname}';" if db_type == 'postgresql' else f"SHOW DATABASES LIKE '{dbname}';"
        ]
        logger.debug(f"Проверка базы данных: {check_db_cmd}")
        check_db_result = await run_subprocess(check_db_cmd, env)
        if check_db_result.returncode != 0:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Повторить", callback_data="retry_password")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text=f"Ошибка проверки базы {dbname}: {check_db_result.stderr}",
                reply_markup=keyboard
            )
            logger.error(f"Ошибка проверки базы {dbname}: {check_db_result.stderr}")
            return

        await state.update_data(password=password)
        if check_db_result.stdout.strip():
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Да, перезаписать", callback_data="confirm_overwrite")],
                [InlineKeyboardButton(text="Нет", callback_data="cancel_overwrite")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text=f"База данных {dbname} уже существует. Перезаписать её? Все данные будут утеряны.",
                reply_markup=keyboard
            )
            await state.set_state(DeployStates.waiting_for_overwrite_confirmation)
            logger.debug(f"Установлено состояние DeployStates.waiting_for_overwrite_confirmation для пользователя {message.from_user.id}")
        else:
            logger.debug(f"База {dbname} не существует, будет создана")
            success, error = await deploy_dump(
                dump_path, db_type, ip, port, dbname, password, username,
                overwrite_confirmed=False, chat_id=chat_id, progress_message_id=current_message_id
            )
            if success:
                await telegram_bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=current_message_id,
                    text=f"✅ Дамп успешно развёрнут на {ip}:{port}/{dbname}."
                )
                logger.info(f"Деплоймент дампа успешен: {dump_path} на {ip}:{port}/{dbname}")
                if temp_file and temp_file.exists():
                    await unlink_file(temp_file)
                    logger.debug(f"Удалён временный файл: {temp_file}")
                if dump_path.exists() and (not temp_file or dump_path != temp_file):
                    await unlink_file(dump_path)
                    logger.debug(f"Удалён временный дамп: {dump_path}")
                if temp_zip and temp_zip.exists():
                    logger.debug(f"ZIP-архив {temp_zip} оставлен в хранилище")
                await state.clear()
            else:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Повторить", callback_data="retry_password")],
                    [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
                ])
                await telegram_bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=current_message_id,
                    text=f"❌ Ошибка при развёртывании дампа: {error}",
                    reply_markup=keyboard
                )
                logger.error(f"Ошибка деплоймента: {error}")
                logger.debug(f"Дамп сохранён для анализа в {ERROR_DUMPS_DIR}/{dump_path.name}")
    except Exception as e:
        logger.error(f"Ошибка при обработке пароля: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Повторить", callback_data="retry_password")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"❌ Ошибка: {e}",
            reply_markup=keyboard
        )
        if temp_file and temp_file.exists():
            await unlink_file(temp_file)
        logger.debug(f"Дамп сохранён для анализа в {ERROR_DUMPS_DIR}/{dump_path.name}")

@dp.callback_query(lambda c: c.data == "retry_password")
async def retry_password(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к вводу пароля."""
    logger.debug(f"Получен callback retry_password от пользователя {callback.from_user.id}")
    data = await state.get_data()
    if not data:
        await callback.message.edit_text("Сессия истекла. Запустите /backup_deploy заново.")
        await state.clear()
        await callback.answer()
        return
    
    try:
        await callback.message.edit_text(
            "Укажите пароль для подключения к базе данных.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="back_to_username")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_deploy")]
            ])
        )
        await state.set_state(DeployStates.waiting_for_password)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в retry_password: {e}")
        await callback.message.edit_text(f"Ошибка: {e}")

@dp.message(DeployStates.waiting_for_overwrite_confirmation)
async def process_overwrite_confirmation(message: types.Message, state: FSMContext):
    """Обработка подтверждения перезаписи базы."""
    logger.debug(f"Получено подтверждение перезаписи от пользователя {message.from_user.id}")
    data = await state.get_data()
    current_message_id = data.get('current_message_id')
    chat_id = data.get('chat_id')
    confirmation = message.text.strip().lower()
    try:
        if confirmation not in ['да', 'нет']:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Да, перезаписать", callback_data="confirm_overwrite")],
                [InlineKeyboardButton(text="Нет", callback_data="cancel_overwrite")]
            ])
            await telegram_bot.edit_message_text(
                chat_id=chat_id,
                message_id=current_message_id,
                text="Пожалуйста, ответьте 'да' или 'нет'. Перезаписать базу данных?",
                reply_markup=keyboard
            )
            logger.warning(f"Некорректное подтверждение: {confirmation}")
            return
        
        if confirmation == 'да':
            await confirm_overwrite(types.CallbackQuery(
                id=message.message_id,
                from_user=message.from_user,
                message=message,
                data="confirm_overwrite"
            ), state)
        else:
            await cancel_overwrite(types.CallbackQuery(
                id=message.message_id,
                from_user=message.from_user,
                message=message,
                data="cancel_overwrite"
            ), state)
    except Exception as e:
        logger.error(f"Ошибка обработки подтверждения перезаписи: {e}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Да, перезаписать", callback_data="confirm_overwrite")],
            [InlineKeyboardButton(text="Нет", callback_data="cancel_overwrite")]
        ])
        await telegram_bot.edit_message_text(
            chat_id=chat_id,
            message_id=current_message_id,
            text=f"Ошибка обработки подтверждения: {e}",
            reply_markup=keyboard
        )