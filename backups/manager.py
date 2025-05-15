from config.settings import POSTGRES_DBS, MYSQL_DBS, MARIADB_DBS, logger
from backups.postgres import process_postgres_db
from backups.mysql import process_mysql_db
from backups.mariadb import process_mariadb_db
from storage.file_exchange import upload_to_file_exchange
from pathlib import Path
import asyncio

async def backup_job():
    """Запуск запланированного бэкапа параллельно для всех баз."""
    logger.info("Запуск запланированного бэкапа")
    
    # Создаём задачи для всех баз
    tasks = []
    for db in POSTGRES_DBS:
        tasks.append(process_postgres_db(db))
    for db in MYSQL_DBS:
        tasks.append(process_mysql_db(db))
    for db in MARIADB_DBS:
        tasks.append(process_mariadb_db(db))
    
    # Запускаем все задачи параллельно
    results = []
    completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Собираем результаты и логируем ошибки
    for db, result in zip(
        POSTGRES_DBS + MYSQL_DBS + MARIADB_DBS,
        completed_tasks
    ):
        db_name = db.get('dbname', db.get('database', 'unknown'))
        if isinstance(result, Exception):
            logger.error(f"Ошибка обработки базы {db_name}: {result}")
        elif result:
            results.append(result)
        else:
            logger.warning(f"Бэкап для базы {db_name} не создан")
    
    logger.info("Запланированный бэкап завершён")
    return results

async def create_backup_now():
    """Создание бэкапа по запросу с загрузкой на файлообменник параллельно."""
    logger.info("Запуск бэкапа по запросу")
    
    # Создаём задачи для всех баз
    tasks = []
    db_list = []
    for db in POSTGRES_DBS:
        tasks.append(process_postgres_db(db))
        db_list.append(('postgres', db))
    for db in MYSQL_DBS:
        tasks.append(process_mysql_db(db))
        db_list.append(('mysql', db))
    for db in MARIADB_DBS:
        tasks.append(process_mariadb_db(db))
        db_list.append(('mariadb', db))
    
    # Запускаем все задачи параллельно
    results = []
    completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Собираем результаты и загружаем на файлообменник
    for (db_type, db), result in zip(db_list, completed_tasks):
        db_name = db.get('dbname', db.get('database', 'unknown'))
        if isinstance(result, Exception):
            logger.error(f"Ошибка создания бэкапа {db_type} {db_name}: {result}")
            continue
        if not result:
            logger.warning(f"Бэкап для {db_type} {db_name} не создан")
            continue
        
        try:
            zip_file = DUMPS_DIR / db_name / result['archive']
            download_url = await upload_to_file_exchange(zip_file)
            result['download_url'] = download_url
            results.append(result)
        except Exception as e:
            logger.error(f"Ошибка загрузки бэкапа {db_type} {db_name} на файлообменник: {e}")
            result['download_url'] = None
            results.append(result)
    
    logger.info("Бэкап по запросу завершён")
    return results