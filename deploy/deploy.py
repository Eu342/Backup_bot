from config.settings import telegram_bot, logger, ERROR_DUMPS_DIR
from backups.utils import run_subprocess
import os
import asyncio
from pathlib import Path
import shutil

async def deploy_dump(dump_path, db_type, ip, port, dbname, password, username, overwrite_confirmed, chat_id, progress_message_id):
    """Развёртывание дампа на удалённый сервер."""
    try:
        env = os.environ.copy()
        env['PGPASSWORD' if db_type == 'postgresql' else 'MYSQL_PWD'] = password
        
        if overwrite_confirmed:
            logger.debug(f"Перезапись базы {dbname} на {ip}:{port}")
            if db_type == 'postgresql':
                drop_cmd = [
                    'psql',
                    '-h', ip,
                    '-p', port,
                    '-U', username,
                    '-d', 'postgres',
                    '-c', f'DROP DATABASE IF EXISTS {dbname};'
                ]
                drop_result = await run_subprocess(drop_cmd, env)
                if drop_result.returncode != 0:
                    logger.error(f"Ошибка удаления базы PostgreSQL {dbname}: {drop_result.stderr}")
                    return False, f"Ошибка удаления базы: {drop_result.stderr}"
                
                create_cmd = [
                    'psql',
                    '-h', ip,
                    '-p', port,
                    '-U', username,
                    '-d', 'postgres',
                    '-c', f'CREATE DATABASE {dbname};'
                ]
                create_result = await run_subprocess(create_cmd, env)
                if create_result.returncode != 0:
                    logger.error(f"Ошибка создания базы PostgreSQL {dbname}: {create_result.stderr}")
                    return False, f"Ошибка создания базы: {create_result.stderr}"
            else:
                drop_cmd = [
                    'mysql',
                    '-h', ip,
                    '-P', port,
                    '-u', username,
                    '-e', f'DROP DATABASE IF EXISTS {dbname};'
                ]
                drop_result = await run_subprocess(drop_cmd, env)
                if drop_result.returncode != 0:
                    logger.error(f"Ошибка удаления базы MySQL {dbname}: {drop_result.stderr}")
                    return False, f"Ошибка удаления базы: {drop_result.stderr}"
                
                create_cmd = [
                    'mysql',
                    '-h', ip,
                    '-P', port,
                    '-u', username,
                    '-e', f'CREATE DATABASE {dbname};'
                ]
                create_result = await run_subprocess(create_cmd, env)
                if create_result.returncode != 0:
                    logger.error(f"Ошибка создания базы MySQL {dbname}: {create_result.stderr}")
                    return False, f"Ошибка создания базы: {create_result.stderr}"
        
        if db_type == 'postgresql':
            cmd = [
                'psql',
                '-h', ip,
                '-p', port,
                '-U', username,
                '-d', dbname,
                '-f', str(dump_path)
            ]
        else:
            cmd = [
                'mysql',
                '-h', ip,
                '-P', port,
                '-u', username,
                '-D', dbname,
                f'--execute=source {dump_path}'
            ]
        
        logger.debug(f"Выполнение команды деплоя: {cmd}")
        result = await run_subprocess(cmd, env)
        if result.returncode != 0:
            logger.error(f"Ошибка развёртывания дампа: {result.stderr}")
            error_dump_path = ERROR_DUMPS_DIR / dump_path.name
            ERROR_DUMPS_DIR.mkdir(exist_ok=True)
            shutil.copy(dump_path, error_dump_path)
            logger.debug(f"Дамп сохранён для анализа в {error_dump_path}")
            return False, f"Ошибка развёртывания: {result.stderr}"
        
        logger.info(f"Успешно развёрнут дамп {dump_path} на {ip}:{port}/{dbname}")
        return True, None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при развёртывании дампа: {e}")
        error_dump_path = ERROR_DUMPS_DIR / dump_path.name
        ERROR_DUMPS_DIR.mkdir(exist_ok=True)
        shutil.copy(dump_path, error_dump_path)
        logger.debug(f"Дамп сохранён для анализа в {error_dump_path}")
        return False, str(e)