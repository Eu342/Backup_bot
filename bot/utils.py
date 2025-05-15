from config.settings import logger, telegram_bot, ADMIN_LIST
from aiogram.types import BotCommand
import asyncio

async def send_telegram_notification(message):
    """Отправка уведомления админам через Telegram."""
    if not telegram_bot:
        logger.error("Telegram-бот не инициализирован, уведомления не отправлены")
        return
    
    for admin_id in ADMIN_LIST:
        try:
            await telegram_bot.send_message(chat_id=admin_id, text=message)
            logger.info(f"Отправлено уведомление админу {admin_id}")
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")

async def set_bot_commands():
    """Установка команд Telegram-бота."""
    if not telegram_bot:
        logger.error("Telegram-бот не инициализирован, команды не установлены")
        return
    
    commands = [
        BotCommand(command="/backup_deploy", description="Развернуть бэкап"),
        BotCommand(command="/backup_create", description="Создать бэкап")
    ]
    try:
        await telegram_bot.set_my_commands(commands)
        logger.info("Команды бота установлены: /backup_deploy, /backup_create")
    except Exception as e:
        logger.error(f"Не удалось установить команды бота: {e}")