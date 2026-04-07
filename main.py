import asyncio
import logging
import os
import sqlite3
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bot.db"
EMOJI_DIR = BASE_DIR / "emoji"

EMOJI_DIR.mkdir(exist_ok=True)


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


conn = get_connection()
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS emoji (
        name TEXT PRIMARY KEY,
        file_path TEXT NOT NULL,
        telegram_file_id TEXT,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
)
conn.commit()


def to_relative_project_path(path: Path) -> str:
    return path.relative_to(BASE_DIR).as_posix()


def from_relative_project_path(path: str) -> Path:
    return BASE_DIR / Path(path)


def scan_emoji_files() -> dict[str, Path]:
    emoji_map: dict[str, Path] = {}
    for file in EMOJI_DIR.iterdir():
        if not file.is_file():
            continue
        emoji_map[file.stem.lower()] = file
    return emoji_map


def sync_emoji_db() -> dict[str, Path]:
    emoji_map = scan_emoji_files()
    db_rows = cursor.execute("SELECT name, file_path FROM emoji").fetchall()
    db_names = {row["name"] for row in db_rows}
    disk_names = set(emoji_map)

    for name, file_path in emoji_map.items():
        relative_path = to_relative_project_path(file_path)
        row = cursor.execute(
            "SELECT file_path FROM emoji WHERE name = ?",
            (name,),
        ).fetchone()

        if row is None:
            cursor.execute(
                """
                INSERT INTO emoji (name, file_path)
                VALUES (?, ?)
                """,
                (name, relative_path),
            )
            logger.info("Р”РѕР±Р°РІРёР» %s РІ Р±Р°Р·Сѓ", name)
            continue

        if row["file_path"] != relative_path:
            cursor.execute(
                """
                UPDATE emoji
                SET file_path = ?, telegram_file_id = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE name = ?
                """,
                (relative_path, name),
            )
            logger.info("РћР±РЅРѕРІРёР» РїСѓС‚СЊ РґР»СЏ %s", name)

    for name in db_names - disk_names:
        cursor.execute("DELETE FROM emoji WHERE name = ?", (name,))
        logger.info("РЈРґР°Р»РёР» %s РёР· Р±Р°Р·С‹, РїРѕС‚РѕРјСѓ С‡С‚Рѕ С„Р°Р№Р»Р° Р±РѕР»СЊС€Рµ РЅРµС‚", name)

    conn.commit()
    return emoji_map


def get_emoji_record(name: str):
    return cursor.execute(
        """
        SELECT name, file_path, telegram_file_id
        FROM emoji
        WHERE name = ?
        """,
        (name,),
    ).fetchone()


def save_telegram_file_id(name: str, telegram_file_id: str):
    cursor.execute(
        """
        UPDATE emoji
        SET telegram_file_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE name = ?
        """,
        (telegram_file_id, name),
    )
    conn.commit()


def find_matching_names(text: str, available_names: list[str]) -> list[str]:
    return [name for name in available_names if name in text]


TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN РЅРµ РЅР°Р№РґРµРЅ. РЈРєР°Р¶РёС‚Рµ РµРіРѕ РІ РїРµСЂРµРјРµРЅРЅРѕР№ РѕРєСЂСѓР¶РµРЅРёСЏ.")


bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
router = Router()


@router.message(Command("help"))
async def help_command(message: Message):
    emoji_map = sync_emoji_db()
    names = sorted(emoji_map)
    if not names:
        await message.answer("Р’ РїР°РїРєРµ emoji РїРѕРєР° РЅРµС‚ С„Р°Р№Р»РѕРІ.")
        return

    await message.answer("РЎРїРёСЃРѕРє СЌРјРѕРґР·Рё:\n" + ", ".join(names))


@router.message(F.text)
async def handle_text(message: Message):
    emoji_map = sync_emoji_db()
    text = message.text.lower()
    matching_names = find_matching_names(text, list(emoji_map))

    for name in matching_names:
        if text == f"{name}.":
            await message.delete()

        record = get_emoji_record(name)
        if record is None:
            continue

        file_path = from_relative_project_path(record["file_path"])
        if not file_path.exists():
            logger.warning("Р¤Р°Р№Р» %s РїСЂРѕРїР°Р» СЃ РґРёСЃРєР°, СЃРёРЅС…СЂРѕРЅРёР·РёСЂСѓСЋ Р±Р°Р·Сѓ", file_path)
            sync_emoji_db()
            continue

        if record["telegram_file_id"]:
            logger.info("РћС‚РїСЂР°РІР»СЏСЋ %s РїРѕ cached file_id", name)
            if file_path.suffix.lower() == ".gif":
                await message.answer_animation(record["telegram_file_id"])
            else:
                await message.answer_sticker(record["telegram_file_id"])
            continue

        logger.info("РћС‚РїСЂР°РІР»СЏСЋ %s РёР· С„Р°Р№Р»Р° %s", name, file_path)
        if file_path.suffix.lower() == ".gif":
            sent_message = await message.answer_animation(FSInputFile(str(file_path)))
            telegram_file_id = sent_message.animation.file_id
        else:
            sent_message = await message.answer_sticker(
                sticker=FSInputFile(str(file_path))
            )
            telegram_file_id = sent_message.sticker.file_id

        save_telegram_file_id(name, telegram_file_id)


async def main():
    sync_emoji_db()
    logger.info("Р—Р°РїСѓСЃРє Р±РѕС‚Р°...")
    logger.info("BOT_TOKEN: %s...", TOKEN[:5])
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("РџСЂРѕРіСЂР°РјРјР° Р·Р°РІРµСЂС€РµРЅР°!")
    finally:
        conn.close()

