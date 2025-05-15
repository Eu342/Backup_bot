from config.settings import logger, PORT, DUMP_INTERVAL_HOURS, telegram_bot, dp
from backups.manager import backup_job
from storage.yandex_disk import cleanup_yandex_disk_backups
from bot.utils import set_bot_commands
from aiogram.exceptions import TelegramNetworkError
import asyncio
import bot.handlers  # Импорт обработчиков

async def start_bot():
    """Запуск опроса Telegram-бота."""
    if not telegram_bot or not dp:
        logger.error("Telegram-бот или Dispatcher не инициализированы (отсутствует TELEGRAM_BOT_TOKEN)")
        return
    
    try:
        logger.debug(f"Запуск polling для dp id: {id(dp)}")
        await set_bot_commands()
        await dp.start_polling(telegram_bot, timeout=60)
    except TelegramNetworkError as e:
        logger.error(f"Сетевая ошибка Telegram: {e}")
        await asyncio.sleep(5)
        await start_bot()
    except Exception as e:
        logger.error(f"Ошибка опроса Telegram-бота: {e}")
        await asyncio.sleep(10)
        await start_bot()
    finally:
        if telegram_bot.session and not telegram_bot.session.closed:
            await telegram_bot.session.close()
            logger.debug("Сессия бота закрыта")

async def run_backups():
    """Периодический запуск запланированных бэкапов."""
    logger.info(f"Запуск цикла бэкапов с интервалом {DUMP_INTERVAL_HOURS} часов")
    while True:
        await backup_job()
        logger.debug(f"Ожидание следующего бэкапа через {DUMP_INTERVAL_HOURS * 3600} секунд")
        await asyncio.sleep(DUMP_INTERVAL_HOURS * 3600)

async def run_yandex_cleanup():
    """Периодический запуск очистки старых бэкапов на Яндекс.Диске."""
    logger.info("Запуск цикла очистки бэкапов на Яндекс.Диске раз в день")
    while True:
        await cleanup_yandex_disk_backups()
        logger.debug("Ожидание следующей очистки Яндекс.Диска через 86400 секунд")
        await asyncio.sleep(86400)

async def main():
    """Основная функция для одновременного запуска бэкапов, очистки и Telegram-бота."""
    logger.info(f"Запуск приложения для бэкапов на порту {PORT}")
    logger.info(f"Интервал бэкапа: каждые {DUMP_INTERVAL_HOURS} часов")
    
    tasks = [
        asyncio.create_task(run_backups()),
        asyncio.create_task(run_yandex_cleanup()),
        asyncio.create_task(start_bot())
    ]
    
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    finally:
        if telegram_bot and telegram_bot.session and not telegram_bot.session.closed:
            asyncio.run(telegram_bot.session.close())
            logger.debug("Сессия бота закрыта при завершении")