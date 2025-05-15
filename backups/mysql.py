import os
from datetime import datetime
from config.settings import DUMPS_DIR, MIN_DUMP_SIZE, logger, YANDEX_DISK_BACKUP_FOLDER
from backups.utils import get_file_size, read_file_lines, unlink_file, run_subprocess, async_archive_dump
from bot.utils import send_telegram_notification
from storage.yandex_disk import upload_to_yandex_disk_rest

async def create_mysql_dump(db_config):
    """Создание дампа базы MySQL."""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_dir = DUMPS_DIR / db_config['database']
        db_dir.mkdir(parents=True, exist_ok=True)
        dump_file = db_dir / f"{db_config['database']}_{timestamp}.sql"
        env = os.environ.copy()
        env['MYSQL_PWD'] = db_config['password']
        cmd = [
            'mysqldump',
            '-h', db_config['host'],
            '-P', db_config['port'],
            '-u', db_config['user'],
            '--no-tablespaces',
            '--single-transaction',
            '--result-file', str(dump_file),
            db_config['database']
        ]
        logger.debug("Перед вызовом run_subprocess: %s", cmd)
        result = await run_subprocess(cmd, env)
        logger.debug("После run_subprocess, результат: returncode=%s", result.returncode)
        if result.returncode != 0:
            raise Exception(f"mysqldump failed: {result.stderr}")
        logger.info(f"Создан дамп MySQL: {dump_file}")
        
        if dump_file.exists():
            file_size = await get_file_size(dump_file)
            if file_size <= MIN_DUMP_SIZE:
                logger.warning(f"Дамп MySQL {dump_file} слишком мал ({file_size} байт), удаляется")
                await unlink_file(dump_file)
                return None
            try:
                first_lines = await read_file_lines(dump_file)
                logger.debug(f"Первые строки дампа MySQL {dump_file}:\n{first_lines}")
            except Exception as e:
                logger.warning(f"Не удалось прочитать первые строки дампа MySQL {dump_file}: {e}")
        else:
            logger.warning(f"Дамп MySQL {dump_file} не был создан")
            return None
        
        return dump_file
    except Exception as e:
        logger.error(f"Ошибка при создании дампа MySQL для {db_config['database']}: {e}")
        if dump_file.exists():
            logger.warning(f"Удаление неудавшегося дампа MySQL {dump_file}")
            await unlink_file(dump_file)
        return None

async def check_mysql_dump(dump_file, db_config):
    """Проверка валидности дампа MySQL."""
    try:
        logger.debug(f"Проверка дампа MySQL: {dump_file}")
        file_size = await get_file_size(dump_file)
        if file_size <= MIN_DUMP_SIZE:
            logger.error(f"Дамп MySQL {dump_file} слишком мал: {file_size} байт")
            return False
        
        env = os.environ.copy()
        env['MYSQL_PWD'] = db_config['password']
        cmd = [
            'mysql',
            '-h', db_config['host'],
            '-P', db_config['port'],
            '-u', db_config['user'],
            '-D', db_config['database'],
            '--batch',
            '-e', f"source {dump_file}"
        ]
        result = await run_subprocess(cmd, env)
        if result.returncode != 0:
            logger.error(f"Дамп MySQL {dump_file} невалиден: {result.stderr}")
            return False
        
        logger.info(f"Дамп MySQL {dump_file} валиден, размер OK: {file_size} байт")
        return True
    except Exception as e:
        logger.error(f"Не удалось проверить дамп MySQL {dump_file}: {e}")
        return False

async def process_mysql_db(db):
    """Обработка бэкапа для одной MySQL базы."""
    try:
        logger.debug(f"Обработка MySQL базы: {db['database']}")
        dump_file = await create_mysql_dump(db)
        if dump_file and await check_mysql_dump(dump_file, db):
            zip_file = await async_archive_dump(dump_file)
            if zip_file:
                logger.debug(f"Попытка загрузки {zip_file} на Яндекс.Диск")
                yandex_uploaded = await upload_to_yandex_disk_rest(zip_file, db['database'])
                message = f"[Фоновый бэкап] ✅ Успешный бэкап MySQL\nБаза: {db['database']}\nАрхив: {zip_file.name}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                if yandex_uploaded:
                    message += f"\nЗагружен на Яндекс.Диск: {YANDEX_DISK_BACKUP_FOLDER}/{db['database']}/{zip_file.name}"
                logger.debug(f"Отправка уведомления для MySQL: {message}")
                await send_telegram_notification(message)
                logger.debug("Уведомление для MySQL отправлено")
        else:
            if dump_file and dump_file.exists():
                logger.warning(f"Удаление невалидного или малого дампа MySQL {dump_file}")
                await unlink_file(dump_file)
    except Exception as e:
        logger.error(f"Не удалось создать бэкап для MySQL базы {db['database']}: {e}")