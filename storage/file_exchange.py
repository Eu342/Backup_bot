import aiohttp
from config.settings import FILE_EXCHANGE_API_URL, logger

async def upload_to_file_exchange(zip_file):
    """Загрузка ZIP-файла на файлообменник и возврат URL для скачивания."""
    if not FILE_EXCHANGE_API_URL:
        logger.warning("Загрузка на файлообменник не настроена (отсутствует FILE_EXCHANGE_API_URL)")
        return None
    
    async with aiohttp.ClientSession() as session:
        try:
            with open(zip_file, 'rb') as f:
                form = aiohttp.FormData()
                form.add_field('file', f, filename=zip_file.name, content_type='application/zip')
                async with session.post(FILE_EXCHANGE_API_URL, data=form, timeout=30) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if 'url' not in data:
                        logger.error(f"Ответ файлообменника не содержит 'url': {data}")
                        return None
                    
                    download_url = data['url']
                    logger.info(f"Загружен {zip_file} на файлообменник: {download_url}")
                    return download_url
        except aiohttp.ClientError as e:
            logger.error(f"Сетевая ошибка при загрузке {zip_file} на файлообменник: {e}")
            return None
        except ValueError as e:
            logger.error(f"Ошибка разбора ответа файлообменника: {e}")
            return None
        except Exception as e:
            logger.error(f"Не удалось загрузить {zip_file} на файлообменник: {e}")
            return None