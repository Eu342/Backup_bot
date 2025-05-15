import zipfile
import asyncio
from config.settings import DUMPS_DIR, MIN_DUMP_SIZE, logger
from pathlib import Path
from datetime import datetime, timedelta, timezone

async def run_subprocess(cmd, env):
    """Run subprocess using asyncio to avoid blocking."""
    logger.debug("Вызов run_subprocess с командой: %s", cmd)
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )
    stdout, stderr = await process.communicate()
    return type('CompletedProcess', (), {
        'returncode': process.returncode,
        'stdout': stdout.decode(),
        'stderr': stderr.decode()
    })()

async def get_file_size(dump_file):
    """Get file size in a separate thread."""
    return await asyncio.to_thread(lambda: dump_file.stat().st_size)

async def read_file_lines(dump_file, num_lines=10):
    """Read first N lines of file in a separate thread."""
    return await asyncio.to_thread(lambda: ''.join(open(dump_file, 'r', encoding='utf-8').readlines()[:num_lines]))

async def unlink_file(file_path):
    """Delete file in a separate thread."""
    if file_path.exists():
        await asyncio.to_thread(file_path.unlink)
        logger.debug(f"Удалён файл: {file_path}")

async def async_archive_dump(dump_file):
    """Архивирование дампа в ZIP и удаление оригинала в отдельном потоке."""
    try:
        zip_file = dump_file.with_suffix('.zip')
        await asyncio.to_thread(lambda: zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED, compresslevel=9).write(dump_file, dump_file.name))
        logger.info(f"Архивирован дамп в: {zip_file}")
        await unlink_file(dump_file)
        return zip_file
    except Exception as e:
        logger.error(f"Не удалось архивировать {dump_file}: {e}")
        return None

def cleanup_old_archives():
    """Удаление ZIP-архивов старше 30 дней."""
    threshold = datetime.now(timezone.utc) - timedelta(days=30)
    for db_dir in DUMPS_DIR.iterdir():
        if db_dir.is_dir():
            for zip_file in db_dir.glob('*.zip'):
                mtime = datetime.fromtimestamp(zip_file.stat().st_mtime, tz=timezone.utc)
                if mtime < threshold:
                    try:
                        zip_file.unlink()
                        logger.info(f"Удалён старый архив: {zip_file}")
                    except Exception as e:
                        logger.error(f"Не удалось удалить {zip_file}: {e}")