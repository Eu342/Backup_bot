import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

# Загрузка .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
BASE_DIR = Path(__file__).resolve().parent.parent
DUMPS_DIR = BASE_DIR / 'dumps'
DUMPS_DIR.mkdir(exist_ok=True)
ERROR_DUMPS_DIR = BASE_DIR / 'dumps' / 'errors'
ERROR_DUMPS_DIR.mkdir(exist_ok=True)
MIN_DUMP_SIZE = 1024
PORT = int(os.getenv('PORT', 7967))
DUMP_INTERVAL_HOURS = int(os.getenv('DUMP_INTERVAL_HOURS', 1))

# Переменные окружения
YANDEX_DISK_TOKEN = os.getenv('YANDEX_DISK_TOKEN', '')
YANDEX_DISK_BACKUP_FOLDER = os.getenv('YANDEX_DISK_BACKUP_FOLDER', '/Backups')
FILE_EXCHANGE_API_URL = os.getenv('FILE_EXCHANGE_API_URL', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ADMIN_LIST = os.getenv('ADMIN_LIST', '').split(',')

# Инициализация бота
telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
dp = Dispatcher(storage=MemoryStorage()) if telegram_bot else None
logger.debug(f"Инициализирован dp с id: {id(dp) if dp else None}")

# Конфигурация баз данных
POSTGRES_DBS = []
MYSQL_DBS = []
MARIADB_DBS = []
ALL_DBS = []  # Список всех баз с типами
for i in range(1, 10):
    pg_db = {
        'dbname': os.getenv(f'POSTGRES_DB_{i}_NAME'),
        'host': os.getenv(f'POSTGRES_DB_{i}_HOST'),
        'port': os.getenv(f'POSTGRES_DB_{i}_PORT', '5432'),
        'user': os.getenv(f'POSTGRES_DB_{i}_USER'),
        'password': os.getenv(f'POSTGRES_DB_{i}_PASSWORD')
    }
    if all([pg_db['dbname'], pg_db['host'], pg_db['user'], pg_db['password']]):
        POSTGRES_DBS.append(pg_db)
        ALL_DBS.append({'name': pg_db['dbname'], 'type': 'PostgreSQL', 'config': pg_db})
        logger.debug(f"Загружена PostgreSQL база {i}: {pg_db['dbname']}")
    
    mysql_db = {
        'database': os.getenv(f'MYSQL_DB_{i}_NAME'),
        'host': os.getenv(f'MYSQL_DB_{i}_HOST'),
        'port': os.getenv(f'MYSQL_DB_{i}_PORT', '3306'),
        'user': os.getenv(f'MYSQL_DB_{i}_USER'),
        'password': os.getenv(f'MYSQL_DB_{i}_PASSWORD')
    }
    if all([mysql_db['database'], mysql_db['host'], mysql_db['user'], mysql_db['password']]):
        MYSQL_DBS.append(mysql_db)
        ALL_DBS.append({'name': mysql_db['database'], 'type': 'MySQL', 'config': mysql_db})
        logger.debug(f"Загружена MySQL база {i}: {mysql_db['database']}")
    
    mariadb_db = {
        'database': os.getenv(f'MARIADB_DB_{i}_NAME'),
        'host': os.getenv(f'MARIADB_DB_{i}_HOST'),
        'port': os.getenv(f'MARIADB_DB_{i}_PORT', '3306'),
        'user': os.getenv(f'MARIADB_DB_{i}_USER'),
        'password': os.getenv(f'MARIADB_DB_{i}_PASSWORD')
    }
    if all([mariadb_db['database'], mariadb_db['host'], mariadb_db['user'], mariadb_db['password']]):
        MARIADB_DBS.append(mariadb_db)
        ALL_DBS.append({'name': mariadb_db['database'], 'type': 'MariaDB', 'config': mariadb_db})
        logger.debug(f"Загружена MariaDB база {i}: {mariadb_db['database']}")

logger.debug(f"Всего загружено: {len(POSTGRES_DBS)} PostgreSQL баз, {len(MYSQL_DBS)} MySQL баз, {len(MARIADB_DBS)} MariaDB баз")