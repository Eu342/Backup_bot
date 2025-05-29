import aiohttp
import os
from config.settings import YANDEX_DISK_TOKEN, YANDEX_DISK_BACKUP_FOLDER, logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from datetime import datetime, timedelta, timezone
import traceback

@retry(stop=stop_after_attempt(3), wait=wait_fixed(10), retry=retry_if_exception_type(aiohttp.ClientError))
async def upload_to_yandex_disk_rest(zip_file, db_name):
    """Загрузка ZIP-файла на Яндекс.Диск через REST API с aiohttp."""
    if not YANDEX_DISK_TOKEN or not YANDEX_DISK_BACKUP_FOLDER:
        logger.debug("Загрузка на Яндекс.Диск отключена (отсутствует YANDEX_DISK_TOKEN или YANDEX_DISK_BACKUP_FOLDER)")
        return False
    
    start_time = datetime.now(timezone.utc)
    file_size = os.path.getsize(zip_file) / 1_048_576  # Размер в МБ
    logger.debug(f"Начало загрузки {zip_file} на Яндекс.Диск: {start_time}, размер: {file_size:.2f} МБ")
    
    async with aiohttp.ClientSession() as session:
        try:
            headers = {"Authorization": f"OAuth {YANDEX_DISK_TOKEN}"}
            
            # Проверка токена
            async with session.get("https://cloud-api.yandex.net/v1/disk", headers=headers, timeout=5) as token_response:
                if token_response.status != 200:
                    logger.error(f"Невалидный токен Яндекс.Диска: {token_response.status} {await token_response.text()}")
                    raise aiohttp.ClientError(f"Invalid token: {token_response.status}")
                logger.debug(f"Токен Яндекс.Диска валиден: {token_response.status}")
            
            # Проверка сети
            try:
                async with session.get("https://cloud-api.yandex.net/ping", timeout=5) as ping_response:
                    logger.debug(f"Пинг до Яндекс.Диска: {ping_response.status}")
            except Exception as ping_err:
                logger.warning(f"Не удалось проверить пинг до Яндекс.Диска: {ping_err}")
            
            folder_url = f"https://cloud-api.yandex.net/v1/disk/resources?path=disk:{YANDEX_DISK_BACKUP_FOLDER}"
            async with session.get(folder_url, headers=headers, timeout=10) as folder_response:
                if folder_response.status == 404:
                    create_folder_url = f"https://cloud-api.yandex.net/v1/disk/resources?path=disk:{YANDEX_DISK_BACKUP_FOLDER}"
                    async with session.put(create_folder_url, headers=headers, timeout=10) as create_response:
                        create_response.raise_for_status()
                        logger.info(f"Создана корневая папка на Яндекс.Диске: {YANDEX_DISK_BACKUP_FOLDER}")
                elif folder_response.status != 200:
                    logger.error(f"Ошибка проверки корневой папки: {folder_response.status} {await folder_response.text()}")
                    raise aiohttp.ClientError(f"Root folder check failed: {await folder_response.text()}")
            
            db_folder_path = f"{YANDEX_DISK_BACKUP_FOLDER}/{db_name}"
            db_folder_url = f"https://cloud-api.yandex.net/v1/disk/resources?path=disk:{db_folder_path}"
            async with session.get(db_folder_url, headers=headers, timeout=10) as db_folder_response:
                if db_folder_response.status == 404:
                    create_db_folder_url = f"https://cloud-api.yandex.net/v1/disk/resources?path=disk:{db_folder_path}"
                    async with session.put(create_db_folder_url, headers=headers, timeout=10) as create_db_response:
                        create_db_response.raise_for_status()
                        logger.info(f"Создана папка базы данных на Яндекс.Диске: {db_folder_path}")
                elif db_folder_response.status != 200:
                    logger.error(f"Ошибка проверки папки базы данных: {db_folder_response.status} {await db_folder_response.text()}")
                    raise aiohttp.ClientError(f"DB folder check failed: {await db_folder_response.text()}")
            
            remote_path = f"{db_folder_path}/{zip_file.name}"
            file_check_url = f"https://cloud-api.yandex.net/v1/disk/resources?path=disk:{remote_path}"
            async with session.get(file_check_url, headers=headers, timeout=10) as file_check_response:
                if file_check_response.status == 200:
                    logger.warning(f"Файл {remote_path} уже существует на Яндекс.Диске, пропускаем загрузку")
                    return False
            
            upload_url = f"https://cloud-api.yandex.net/v1/disk/resources/upload?path=disk:{remote_path}&overwrite=false"
            async with session.get(upload_url, headers=headers, timeout=10) as upload_response:
                upload_response.raise_for_status()
                upload_data = await upload_response.json()
                put_url = upload_data.get("href")
                if not put_url:
                    logger.error("Не удалось получить URL для загрузки от Яндекс.Диска")
                    raise aiohttp.ClientError("No upload URL")
            
            with open(zip_file, 'rb') as f:
                async with session.put(put_url, data=f, chunked=True, timeout=600) as put_response:
                    put_response.raise_for_status()
            
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Загружен файл {zip_file} на Яндекс.Диск: {remote_path}, размер: {file_size:.2f} МБ, время: {duration:.2f} сек")
            return True
        except aiohttp.ClientError as e:
            logger.error(f"Сетевая ошибка при загрузке {zip_file} на Яндекс.Диск: {e}\n{traceback.format_exc()}")
            return False
        except Exception as e:
            logger.error(f"Не удалось загрузить {zip_file} на Яндекс.Диск: {e}\n{traceback.format_exc()}")
            return False

async def cleanup_yandex_disk_backups():
    """Ежедневная очистка бэкапов старше 31 дня на Яндекс.Диске."""
    if not YANDEX_DISK_TOKEN or not YANDEX_DISK_BACKUP_FOLDER:
        logger.debug("Очистка Яндекс.Диска отключена (отсутствует YANDEX_DISK_TOKEN или YANDEX_DISK_BACKUP_FOLDER)")
        return
    
    async with aiohttp.ClientSession() as session:
        try:
            headers = {"Authorization": f"OAuth {YANDEX_DISK_TOKEN}"}
            threshold = datetime.now(timezone.utc) - timedelta(days=31)
            
            folder_url = f"https://cloud-api.yandex.net/v1/disk/resources?path=disk:{YANDEX_DISK_BACKUP_FOLDER}&limit=1000"
            async with session.get(folder_url, headers=headers, timeout=10) as folder_response:
                folder_response.raise_for_status()
                folder_data = await folder_response.json()
            
            for item in folder_data.get('_embedded', {}).get('items', []):
                if item['type'] != 'dir':
                    continue
                db_folder_path = item['path'].replace('disk:', '')
                
                files_url = f"https://cloud-api.yandex.net/v1/disk/resources?path={db_folder_path}&limit=1000"
                async with session.get(files_url, headers=headers, timeout=10) as files_response:
                    files_response.raise_for_status()
                    files_data = await files_response.json()
                
                for file_item in files_data.get('_embedded', {}).get('items', []):
                    if file_item['type'] != 'file':
                        continue
                    modified_str = file_item['modified']
                    modified_time = datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
                    if modified_time < threshold:
                        try:
                            delete_url = f"https://cloud-api.yandex.net/v1/disk/resources?path={file_item['path']}&permanently=true"
                            async with session.delete(delete_url, headers=headers, timeout=10) as delete_response:
                                delete_response.raise_for_status()
                                logger.info(f"Удалён старый бэкап на Яндекс.Диске: {file_item['path']}")
                        except Exception as e:
                            logger.error(f"Не удалось удалить {file_item['path']} на Яндекс.Диске: {e}")
            
            logger.info("Очистка старых бэкапов на Яндекс.Диске завершена")
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка очистки бэкапов на Яндекс.Диске: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при очистке Яндекс.Диска: {e}")
