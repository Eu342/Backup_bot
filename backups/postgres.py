from config.settings import logger, telegram_bot, DUMPS_DIR, MIN_DUMP_SIZE, YANDEX_DISK_TOKEN, ADMIN_LIST
from backups.utils import run_subprocess, async_archive_dump, unlink_file
from storage.yandex_disk import upload_to_yandex_disk_rest
from bot.utils import send_telegram_notification
from datetime import datetime
import os
import asyncio
from pathlib import Path

async def process_postgres_db(db, is_manual=False):
    """Создание дампа PostgreSQL базы."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_name = db['dbname']
        dump_dir = DUMPS_DIR / db_name
        dump_dir.mkdir(exist_ok=True)
        dump_file = dump_dir / f"{db_name}_{timestamp}.sql"
        
        env = os.environ.copy()
        env['PGPASSWORD'] = db['password']
        cmd = [
            'pg_dump',
            '-h', db['host'],
            '-p', db['port'],
            '-U', db['user'],
            '-d', db_name,
            '--schema=public',
            '--no-owner',
            '--no-privileges',
            '-f', str(dump_file)
        ]
        logger.debug(f"Создание дампа PostgreSQL: {cmd}")
        result = await run_subprocess(cmd, env)
        
        if result.returncode != 0:
            logger.error(f"Ошибка создания дампа PostgreSQL {db_name}: {result.stderr}")
            if dump_file.exists():
                logger.warning(f"Удаление неудавшегося дампа PostgreSQL {dump_file}")
                await unlink_file(dump_file)
            return None
        
        if not dump_file.exists() or dump_file.stat().st_size < MIN_DUMP_SIZE:
            logger.error(f"Дамп PostgreSQL {db_name} пуст или слишком мал: {dump_file}")
            await unlink_file(dump_file)
            return None
        
        logger.info(f"Дамп PostgreSQL {dump_file} валиден, размер OK: {dump_file.stat().st_size} байт")
        
        zip_file = await async_archive_dump(dump_file)
        if not zip_file:
            logger.error(f"Не удалось заархивировать дамп PostgreSQL {dump_file}")
            await unlink_file(dump_file)
            return None
        
        await unlink_file(dump_file)
        
        yandex_uploaded = False
        if YANDEX_DISK_TOKEN:
            yandex_uploaded = await upload_to_yandex_disk_rest(zip_file, db_name)
        
        if telegram_bot and not is_manual:
            timestamp_formatted = datetime.now().strftime("%H:%M %d.%m.%Y")
            message = (
                f"<b>✅ Создание бэкапа завершено!</b>\n\n"
                f"🗄️ <b>База</b>: {db_name}\n"
                f"📁 <b>Файл</b>: <a href=\"tg://btn/copy_file:{zip_file.name}\"><code>{zip_file.name}</code></a>\n"
                f"📅 <b>Время создания</b>: {timestamp_formatted}\n"
                f"☁️ <b>Я.Диск</b>: /Backups/{db_name}/{zip_file.name}" if yandex_uploaded else
                f"☁️ <b>Я.Диск</b>: Не загружен"
            )
            await telegram_bot.send_message(
                chat_id=ADMIN_LIST[0],
                text=message,
                parse_mode="HTML"
            )
            logger.debug(f"Отправлено фоновое уведомление для {db_name}: {message}")
        
        return {
            'database': db_name,
            'archive': zip_file.name,
            'yandex_uploaded': yandex_uploaded
        }
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании дампа PostgreSQL {db.get('dbname', 'unknown')}: {e}")
        if 'dump_file' in locals() and dump_file.exists():
            await unlink_file(dump_file)
        return None