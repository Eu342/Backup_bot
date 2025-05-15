from config.settings import POSTGRES_DBS, MYSQL_DBS, logger
from backups.postgres import process_postgres_db
from backups.mysql import process_mysql_db
from storage.file_exchange import upload_to_file_exchange
import asyncio

async def backup_job():
    """Запуск запланированного бэкапа."""
    logger.info("Запуск запланированного бэкапа")
    results = []
    
    for db in POSTGRES_DBS:
        try:
            result = await process_postgres_db(db)
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Ошибка обработки PostgreSQL базы {db['dbname']}: {e}")
    
    for db in MYSQL_DBS:
        try:
            result = await process_mysql_db(db)
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Ошибка обработки MySQL базы {db['database']}: {e}")
    
    logger.info("Запланированный бэкап завершён")
    return results

async def create_backup_now():
    """Создание бэкапа по запросу с загрузкой на файлообменник."""
    logger.info("Запуск бэкапа по запросу")
    results = []
    
    for db in POSTGRES_DBS:
        try:
            dump_file = await process_postgres_db(db)
            if dump_file:
                zip_file = await async_archive_dump(dump_file)
                if zip_file:
                    download_url = await upload_to_file_exchange(zip_file)
                    results.append({
                        'database': db['dbname'],
                        'archive': zip_file.name,
                        'download_url': download_url
                    })
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа PostgreSQL {db['dbname']}: {e}")
    
    for db in MYSQL_DBS:
        try:
            dump_file = await process_mysql_db(db)
            if dump_file:
                zip_file = await async_archive_dump(dump_file)
                if zip_file:
                    download_url = await upload_to_file_exchange(zip_file)
                    results.append({
                        'database': db['database'],
                        'archive': zip_file.name,
                        'download_url': download_url
                    })
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа MySQL {db['database']}: {e}")
    
    logger.info("Бэкап по запросу завершён")
    return results