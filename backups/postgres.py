import os
from datetime import datetime
import asyncio
from config.settings import DUMPS_DIR, MIN_DUMP_SIZE, logger, YANDEX_DISK_BACKUP_FOLDER
from backups.utils import get_file_size, read_file_lines, unlink_file, run_subprocess, async_archive_dump
from bot.utils import send_telegram_notification
from storage.yandex_disk import upload_to_yandex_disk_rest

async def create_postgres_dump(db_config):
    """Создание дампа базы PostgreSQL."""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_dir = DUMPS_DIR / db_config['dbname']
        db_dir.mkdir(parents=True, exist_ok=True)
        dump_file = db_dir / f"{db_config['dbname']}_{timestamp}.sql"
        env = os.environ.copy()
        env['PGPASSWORD'] = db_config['password']
        cmd = [
            'pg_dump',
            '-h', db_config['host'],
            '-p', db_config['port'],
            '-U', db_config['user'],
            '-F', 'p',
            '--schema=public',
            '--no-owner',
            '--no-privileges',
            '-v',
            '-f', str(dump_file),
            db_config['dbname']
        ]
        logger.debug("Перед вызовом run_subprocess: %s", cmd)
        result = await run_subprocess(cmd, env)
        logger.debug("После run_subprocess, результат: returncode=%s", result.returncode)
        if result.returncode != 0:
            logger.error(f"Ошибка pg_dump: stdout={result.stdout}, stderr={result.stderr}")
            raise Exception(f"pg_dump failed: {result.stderr}")
        logger.info(f"Создан дамп PostgreSQL: {dump_file}")
        
        if dump_file.exists():
            file_size = await get_file_size(dump_file)
            if file_size <= MIN_DUMP_SIZE:
                logger.warning(f"Дамп PostgreSQL {dump_file} слишком мал ({file_size} байт), удаляется")
                await unlink_file(dump_file)
                await send_telegram_notification(f"Дамп {dump_file} слишком мал ({file_size} байт) и удалён")
                return None
            try:
                first_lines = await read_file_lines(dump_file, num_lines=100)
                logger.debug(f"Первые строки дампа PostgreSQL {dump_file}:\n{first_lines}")
            except Exception as e:
                logger.warning(f"Не удалось прочитать первые строки дампа PostgreSQL {dump_file}: {e}")
        else:
            logger.warning(f"Дамп PostgreSQL {dump_file} не был создан")
            await send_telegram_notification(f"Дамп {dump_file} не был создан")
            return None
        
        return dump_file
    except Exception as e:
        logger.error(f"Ошибка при создании дампа PostgreSQL для {db_config['dbname']}: {e}")
        if dump_file.exists():
            logger.warning(f"Удаление неудавшегося дампа PostgreSQL {dump_file}")
            await unlink_file(dump_file)
            await send_telegram_notification(f"Ошибка создания дампа {db_config['dbname']}: {e}")
        return None

async def check_postgres_dump(dump_file, db_config):
    """Проверка валидности дампа PostgreSQL."""
    try:
        logger.debug(f"Проверка дампа PostgreSQL: {dump_file}")
        file_size = await get_file_size(dump_file)
        if file_size <= MIN_DUMP_SIZE:
            logger.error(f"Дамп PostgreSQL {dump_file} слишком мал: {file_size} байт")
            return False
        
        full_content = await asyncio.to_thread(lambda: open(dump_file, 'r', encoding='utf-8').read())
        if not any(keyword in full_content.lower() for keyword in ['create table', 'create sequence']):
            logger.error(f"Дамп PostgreSQL {dump_file} не содержит команд CREATE TABLE или CREATE SEQUENCE")
            return False
        
        logger.info(f"Дамп PostgreSQL {dump_file} валиден, размер OK: {file_size} байт")
        return True
    except Exception as e:
        logger.error(f"Не удалось проверить дамп PostgreSQL {dump_file}: {e}")
        return False

async def process_postgres_db(db):
    """Обработка бэкапа для одной PostgreSQL базы."""
    try:
        logger.debug(f"Обработка PostgreSQL базы: {db['dbname']}")
        dump_file = await create_postgres_dump(db)
        if dump_file and await check_postgres_dump(dump_file, db):
            zip_file = await async_archive_dump(dump_file)
            if zip_file:
                logger.debug(f"Попытка загрузки {zip_file} на Яндекс.Диск")
                yandex_uploaded = await upload_to_yandex_disk_rest(zip_file, db['dbname'])
                message = f"[Фоновый бэкап] ✅ Успешный бэкап PostgreSQL\nБаза: {db['dbname']}\nАрхив: {zip_file.name}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                if yandex_uploaded:
                    message += f"\nЗагружен на Яндекс.Диск: {YANDEX_DISK_BACKUP_FOLDER}/{db['dbname']}/{zip_file.name}"
                logger.debug(f"Отправка уведомления для PostgreSQL: {message}")
                await send_telegram_notification(message)
                logger.debug("Уведомление для PostgreSQL отправлено")
        else:
            if dump_file and dump_file.exists():
                logger.warning(f"Удаление невалидного или малого дампа PostgreSQL {dump_file}")
                await send_telegram_notification(f"Дамп {dump_file} невалиден или слишком мал и удалён")
                await unlink_file(dump_file)
    except Exception as e:
        logger.error(f"Не удалось создать бэкап для PostgreSQL базы {db['dbname']}: {e}")
        await send_telegram_notification(f"Ошибка бэкапа PostgreSQL {db['dbname']}: {e}")