from config.settings import logger, telegram_bot, DUMPS_DIR, MIN_DUMP_SIZE, YANDEX_DISK_TOKEN
from backups.utils import run_subprocess, async_archive_dump, unlink_file
from storage.yandex_disk import upload_to_yandex_disk_rest
from datetime import datetime
import os
import asyncio
from pathlib import Path

async def process_mariadb_db(db):
    """Создание дампа MariaDB базы."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_name = db['database']
        dump_dir = DUMPS_DIR / db_name
        dump_dir.mkdir(exist_ok=True)
        dump_file = dump_dir / f"{db_name}_{timestamp}.sql"
        
        env = os.environ.copy()
        env['MYSQL_PWD'] = db['password']
        cmd = [
            'mysqldump',
            '-h', db['host'],
            '-P', db['port'],
            '-u', db['user'],
            db_name,
            '--single-transaction',
            '--no-tablespaces',
            '--column-statistics=0',  # Отключаем статистику для совместимости
            '-r', str(dump_file)
        ]
        logger.debug(f"Создание дампа MariaDB: {cmd}")
        result = await run_subprocess(cmd, env)
        
        if result.returncode != 0:
            logger.error(f"Ошибка создания дампа MariaDB {db_name}: {result.stderr}")
            return None
        
        if not dump_file.exists() or dump_file.stat().st_size < MIN_DUMP_SIZE:
            logger.error(f"Дамп MariaDB {db_name} пуст или слишком мал: {dump_file}")
            await unlink_file(dump_file)
            return None
        
        logger.info(f"Дамп MariaDB {dump_file} валиден, размер OK: {dump_file.stat().st_size} байт")
        
        zip_file = await async_archive_dump(dump_file)
        if not zip_file:
            logger.error(f"Не удалось заархивировать дамп MariaDB {dump_file}")
            await unlink_file(dump_file)
            return None
        
        await unlink_file(dump_file)
        
        yandex_uploaded = False
        if YANDEX_DISK_TOKEN:
            yandex_uploaded = await upload_to_yandex_disk_rest(zip_file, db_name)
        
        if telegram_bot:
            message = f"[Фоновый бэкап] ✅ Успешный бэкап MariaDB\nБаза: {db_name}\nАрхив: {zip_file.name}\nВремя: {datetime.now()}"
            if yandex_uploaded:
                message += f"\nЗагружен на Яндекс.Диск: /Backups/{db_name}/{zip_file.name}"
            else:
                message += "\nНе загружен на Яндекс.Диск"
            from bot.utils import send_telegram_notification
            await send_telegram_notification(message)
        
        return {
            'database': db_name,
            'archive': zip_file.name,
            'yandex_uploaded': yandex_uploaded
        }
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании дампа MariaDB {db.get('database', 'unknown')}: {e}")
        await unlink_file(dump_file)
        return None