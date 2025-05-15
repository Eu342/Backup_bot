from config.settings import POSTGRES_DBS, MYSQL_DBS, MARIADB_DBS, logger, DUMPS_DIR
from backups.postgres import process_postgres_db
from backups.mysql import process_mysql_db
from backups.mariadb import process_mariadb_db
from storage.file_exchange import upload_to_file_exchange
from pathlib import Path
import asyncio

async def backup_job():
    """Запуск запланированного бэкапа последовательно."""
    logger.info("Запуск запланированного бэкапа")
    results = []
    
    for db in POSTGRES_DBS:
        try:
            result = await process_postgres_db(db, is_manual=False)
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Ошибка обработки PostgreSQL базы {db['dbname']}: {e}")
    
    for db in MYSQL_DBS:
        try:
            result = await process_mysql_db(db, is_manual=False)
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Ошибка обработки MySQL базы {db['database']}: {e}")
    
    for db in MARIADB_DBS:
        try:
            result = await process_mariadb_db(db, is_manual=False)
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Ошибка обработки MariaDB базы {db['database']}: {e}")
    
    logger.info("Запланированный бэкап завершён")
    return results

async def create_backup_now():
    """Создание бэкапа по запросу с загрузкой на файлообменник."""
    logger.info("Запуск бэкапа по запросу")
    results = []
    
    for db in POSTGRES_DBS:
        try:
            result = await process_postgres_db(db, is_manual=True)
            if result:
                zip_file = DUMPS_DIR / db['dbname'] / result['archive']
                download_url = await upload_to_file_exchange(zip_file)
                result['download_url'] = download_url
                results.append(result)
            else:
                logger.warning(f"Бэкап для PostgreSQL {db['dbname']} не создан")
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа PostgreSQL {db['dbname']}: {e}")
    
    for db in MYSQL_DBS:
        try:
            result = await process_mysql_db(db, is_manual=True)
            if result:
                zip_file = DUMPS_DIR / db['database'] / result['archive']
                download_url = await upload_to_file_exchange(zip_file)
                result['download_url'] = download_url
                results.append(result)
            else:
                logger.warning(f"Бэкап для MySQL {db['database']} не создан")
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа MySQL {db['database']}: {e}")
    
    for db in MARIADB_DBS:
        try:
            result = await process_mariadb_db(db, is_manual=True)
            if result:
                zip_file = DUMPS_DIR / db['database'] / result['archive']
                download_url = await upload_to_file_exchange(zip_file)
                result['download_url'] = download_url
                results.append(result)
            else:
                logger.warning(f"Бэкап для MariaDB {db['database']} не создан")
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа MariaDB {db['database']}: {e}")
    
    logger.info("Бэкап по запросу завершён")
    return results

async def create_backup_for_db(db_config, db_type):
    """Создание бэкапа для одной базы."""
    db_name = db_config.get('dbname', db_config.get('database'))
    logger.info(f"Запуск бэкапа для базы {db_name} ({db_type})")
    try:
        if db_type == 'PostgreSQL':
            result = await process_postgres_db(db_config, is_manual=True)
        elif db_type == 'MySQL':
            result = await process_mysql_db(db_config, is_manual=True)
        elif db_type == 'MariaDB':
            result = await process_mariadb_db(db_config, is_manual=True)
        else:
            logger.error(f"Неизвестный тип базы: {db_type}")
            return None
        
        if result:
            zip_file = DUMPS_DIR / result['database'] / result['archive']
            logger.debug(f"Загрузка архива {zip_file} на файлообменник")
            download_url = await upload_to_file_exchange(zip_file)
            result['download_url'] = download_url
            logger.info(f"Бэкап для {result['database']} завершён успешно")
            return result
        else:
            logger.error(f"Бэкап для {db_name} не создан")
            return None
    except Exception as e:
        logger.error(f"Ошибка создания бэкапа {db_type} {db_name}: {e}")
        return None