import asyncio
import hashlib
import json
import logging
import os
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message
from PIL import Image, ImageSequence, UnidentifiedImageError

try:
    import pillow_avif_plugin  # noqa: F401
except ImportError:
    pillow_avif_plugin = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bot.db"
EMOJI_DIR = BASE_DIR / "emoji"
CONVERTED_DIR = BASE_DIR / "converted_emoji"
GITHUB_REPO = os.getenv("GITHUB_REPO", "MorKlein/emoji_bot").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip() or "main"
GITHUB_EMOJI_DIR = os.getenv("GITHUB_EMOJI_DIR", "emoji").strip().strip("/") or "emoji"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()

STICKER_EXTENSIONS = {".webp", ".tgs", ".webm"}
ANIMATION_EXTENSIONS = {".gif"}
RESAMPLING = getattr(Image, "Resampling", Image)

EMOJI_DIR.mkdir(exist_ok=True)
CONVERTED_DIR.mkdir(exist_ok=True)


@dataclass
class PreparedMedia:
    file_path: Path
    media_type: str


@dataclass
class GitHubSyncResult:
    success: bool
    attempted: bool
    remote_files: int
    written_files: int
    removed_files: int
    message: str


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
        telegram_media_type TEXT,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
)
conn.commit()

columns = {
    row["name"]
    for row in cursor.execute("PRAGMA table_info(emoji)").fetchall()
}
if "telegram_media_type" not in columns:
    cursor.execute("ALTER TABLE emoji ADD COLUMN telegram_media_type TEXT")
    conn.commit()


def to_relative_project_path(path: Path) -> str:
    return path.relative_to(BASE_DIR).as_posix()


def from_relative_project_path(path: str) -> Path:
    return BASE_DIR / Path(path)


def build_converted_path(source_path: Path, suffix: str) -> Path:
    digest_source = (
        f"{source_path.resolve()}:"
        f"{source_path.stat().st_mtime_ns}:"
        f"{source_path.stat().st_size}"
    )
    digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:12]
    return CONVERTED_DIR / f"{source_path.stem}_{digest}{suffix}"


def fit_image_to_sticker(image: Image.Image) -> Image.Image:
    prepared = image.convert("RGBA")
    if prepared.width == 0 or prepared.height == 0:
        raise RuntimeError("Image has invalid dimensions for sticker conversion")

    scale = min(512 / prepared.width, 512 / prepared.height)
    resized_size = (
        max(1, round(prepared.width * scale)),
        max(1, round(prepared.height * scale)),
    )
    prepared = prepared.resize(resized_size, RESAMPLING.LANCZOS)

    canvas = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
    offset = (
        (512 - prepared.width) // 2,
        (512 - prepared.height) // 2,
    )
    canvas.paste(prepared, offset, prepared)
    return canvas


def convert_to_sticker(file_path: Path) -> Path:
    target_path = build_converted_path(file_path, ".webp")
    if target_path.exists():
        return target_path

    with Image.open(file_path) as image:
        sticker_image = fit_image_to_sticker(image)
        sticker_image.save(target_path, format="WEBP", lossless=True)

    logger.info("Converted %s to sticker %s", file_path.name, target_path.name)
    return target_path


def convert_to_gif(file_path: Path) -> Path:
    target_path = build_converted_path(file_path, ".gif")
    if target_path.exists():
        return target_path

    with Image.open(file_path) as image:
        frames: list[Image.Image] = []
        durations: list[int] = []

        for frame in ImageSequence.Iterator(image):
            frames.append(frame.convert("RGBA"))
            durations.append(frame.info.get("duration", image.info.get("duration", 100)))

        if not frames:
            raise RuntimeError(f"Could not extract frames from {file_path.name}")

        first_frame, *rest_frames = frames
        first_frame.save(
            target_path,
            format="GIF",
            save_all=True,
            append_images=rest_frames,
            duration=durations,
            loop=0,
            disposal=2,
        )

    logger.info("Converted %s to GIF %s", file_path.name, target_path.name)
    return target_path


def prepare_media_for_sending(file_path: Path) -> PreparedMedia:
    suffix = file_path.suffix.lower()

    if suffix in ANIMATION_EXTENSIONS:
        return PreparedMedia(file_path=file_path, media_type="animation")

    if suffix in STICKER_EXTENSIONS:
        return PreparedMedia(file_path=file_path, media_type="sticker")

    try:
        with Image.open(file_path) as image:
            is_animated = bool(getattr(image, "is_animated", False))
            is_animated = is_animated or getattr(image, "n_frames", 1) > 1
    except UnidentifiedImageError as error:
        raise RuntimeError(
            f"Could not open {file_path.name} for conversion; unsupported format {suffix or '[no extension]'}"
        ) from error

    if is_animated:
        return PreparedMedia(file_path=convert_to_gif(file_path), media_type="animation")

    return PreparedMedia(file_path=convert_to_sticker(file_path), media_type="sticker")


def scan_emoji_files() -> dict[str, Path]:
    emoji_map: dict[str, Path] = {}
    for file in EMOJI_DIR.iterdir():
        if not file.is_file():
            continue
        emoji_map[file.stem.lower()] = file
    return emoji_map


def github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "7tv-emoji-bot",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return headers


def github_get_json(url: str):
    request = Request(url, headers=github_headers())
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def github_get_bytes(url: str) -> bytes:
    request = Request(url, headers=github_headers())
    with urlopen(request, timeout=30) as response:
        return response.read()


def list_github_emoji_files() -> tuple[str, list[PurePosixPath]]:
    branch_url = (
        f"https://api.github.com/repos/{GITHUB_REPO}"
        f"/branches/{quote(GITHUB_BRANCH, safe='')}"
    )
    branch_data = github_get_json(branch_url)
    commit_sha = branch_data["commit"]["sha"]

    tree_url = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{commit_sha}?recursive=1"
    tree_data = github_get_json(tree_url)

    emoji_root = PurePosixPath(GITHUB_EMOJI_DIR)
    files: list[PurePosixPath] = []
    for item in tree_data.get("tree", []):
        if item.get("type") != "blob":
            continue

        item_path = PurePosixPath(item["path"])
        try:
            relative_path = item_path.relative_to(emoji_root)
        except ValueError:
            continue

        if not relative_path.parts:
            continue
        if any(part in {"", ".."} for part in relative_path.parts):
            continue

        files.append(relative_path)

    return commit_sha, files


def sync_emoji_dir_with_github() -> GitHubSyncResult:
    if not GITHUB_REPO:
        return GitHubSyncResult(
            success=False,
            attempted=False,
            remote_files=0,
            written_files=0,
            removed_files=0,
            message="GitHub sync skipped: set GITHUB_REPO to owner/repo.",
        )

    try:
        commit_sha, remote_files = list_github_emoji_files()
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace").strip()
        return GitHubSyncResult(
            success=False,
            attempted=True,
            remote_files=0,
            written_files=0,
            removed_files=0,
            message=f"GitHub sync failed: HTTP {error.code}. {details}",
        )
    except URLError as error:
        return GitHubSyncResult(
            success=False,
            attempted=True,
            remote_files=0,
            written_files=0,
            removed_files=0,
            message=f"GitHub sync failed: {error.reason}",
        )
    except KeyError as error:
        return GitHubSyncResult(
            success=False,
            attempted=True,
            remote_files=0,
            written_files=0,
            removed_files=0,
            message=f"GitHub sync failed: unexpected API response ({error}).",
        )

    remote_set = {path.as_posix() for path in remote_files}

    removed_files = 0
    if EMOJI_DIR.exists():
        for local_path in sorted(EMOJI_DIR.rglob("*"), reverse=True):
            if local_path.is_file():
                relative_path = local_path.relative_to(EMOJI_DIR).as_posix()
                if relative_path not in remote_set:
                    local_path.unlink()
                    removed_files += 1
            elif local_path.is_dir() and not any(local_path.iterdir()):
                local_path.rmdir()

    written_files = 0
    for relative_path in remote_files:
        local_path = EMOJI_DIR.joinpath(*relative_path.parts)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        remote_path = (PurePosixPath(GITHUB_EMOJI_DIR) / relative_path).as_posix()
        raw_url = (
            f"https://raw.githubusercontent.com/{GITHUB_REPO}/{commit_sha}/"
            f"{quote(remote_path, safe='/')}"
        )
        remote_bytes = github_get_bytes(raw_url)
        current_bytes = local_path.read_bytes() if local_path.exists() else None
        if current_bytes != remote_bytes:
            local_path.write_bytes(remote_bytes)
            written_files += 1

    logger.info(
        "Synced emoji folder with GitHub: repo=%s branch=%s remote=%s written=%s removed=%s",
        GITHUB_REPO,
        GITHUB_BRANCH,
        len(remote_files),
        written_files,
        removed_files,
    )
    return GitHubSyncResult(
        success=True,
        attempted=True,
        remote_files=len(remote_files),
        written_files=written_files,
        removed_files=removed_files,
        message=f"GitHub sync completed from {GITHUB_REPO}@{GITHUB_BRANCH}.",
    )


def clear_emoji_state() -> dict[str, int]:
    row_count = cursor.execute("SELECT COUNT(*) AS count FROM emoji").fetchone()["count"]

    cursor.execute("DELETE FROM emoji")
    conn.commit()

    converted_removed = 0
    if CONVERTED_DIR.exists():
        for entry in CONVERTED_DIR.iterdir():
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
            converted_removed += 1

    logger.info("Cleared emoji database and removed %s converted files", converted_removed)
    return {
        "db_rows_removed": row_count,
        "converted_removed": converted_removed,
    }


def sync_emoji_db(reset_file_ids: bool = False) -> tuple[dict[str, Path], dict[str, int]]:
    emoji_map = scan_emoji_files()
    db_rows = cursor.execute("SELECT name, file_path FROM emoji").fetchall()
    db_names = {row["name"] for row in db_rows}
    disk_names = set(emoji_map)
    stats = {
        "added": 0,
        "updated": 0,
        "removed": 0,
        "file_ids_reset": 0,
    }

    for name, file_path in emoji_map.items():
        relative_path = to_relative_project_path(file_path)
        row = cursor.execute(
            """
            SELECT file_path, telegram_file_id, telegram_media_type
            FROM emoji
            WHERE name = ?
            """,
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
            stats["added"] += 1
            logger.info("Added %s to database", name)
            continue

        path_changed = row["file_path"] != relative_path
        should_reset_file_id = reset_file_ids and row["telegram_file_id"] is not None

        if path_changed or should_reset_file_id:
            cursor.execute(
                """
                UPDATE emoji
                SET file_path = ?, telegram_file_id = ?, telegram_media_type = ?, updated_at = CURRENT_TIMESTAMP
                WHERE name = ?
                """,
                (
                    relative_path,
                    None if path_changed or reset_file_ids else row["telegram_file_id"],
                    None if path_changed or reset_file_ids else row["telegram_media_type"],
                    name,
                ),
            )
            if path_changed:
                stats["updated"] += 1
                logger.info("Updated path for %s", name)
            if should_reset_file_id:
                stats["file_ids_reset"] += 1
                logger.info("Reset Telegram file_id for %s", name)

    for name in db_names - disk_names:
        cursor.execute("DELETE FROM emoji WHERE name = ?", (name,))
        stats["removed"] += 1
        logger.info("Removed %s from database because the file no longer exists", name)

    conn.commit()
    return emoji_map, stats


def get_emoji_record(name: str):
    return cursor.execute(
        """
        SELECT name, file_path, telegram_file_id, telegram_media_type
        FROM emoji
        WHERE name = ?
        """,
        (name,),
    ).fetchone()


def save_telegram_file_id(name: str, telegram_file_id: str, media_type: str):
    cursor.execute(
        """
        UPDATE emoji
        SET telegram_file_id = ?, telegram_media_type = ?, updated_at = CURRENT_TIMESTAMP
        WHERE name = ?
        """,
        (telegram_file_id, media_type, name),
    )
    conn.commit()


def find_matching_names(text: str, available_names: list[str]) -> list[str]:
    return [name for name in available_names if name in text]


def get_cached_media_type(record: sqlite3.Row, file_path: Path) -> str:
    if record["telegram_media_type"] in {"animation", "sticker"}:
        return record["telegram_media_type"]

    if file_path.suffix.lower() in ANIMATION_EXTENSIONS:
        return "animation"

    return "sticker"


TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN not found. Set it in the environment.")


bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
router = Router()


@router.message(Command("help"))
async def help_command(message: Message):
    emoji_map, _ = sync_emoji_db()
    names = sorted(emoji_map)

    lines = [
        "https://011b0034.7tv-emoji-site.pages.dev/",
    ]

    if names:
        lines.append("")
        lines.append("Available emoji:")
        lines.append(", ".join(names))
    else:
        lines.append("")
        lines.append("The emoji folder is currently empty.")

    await message.answer("\n".join(lines))


@router.message(Command("update"))
async def update_command(message: Message):
    github_sync = sync_emoji_dir_with_github()
    emoji_map, stats = sync_emoji_db(reset_file_ids=True)
    names = sorted(emoji_map)

    lines = [
        "Emoji list updated.",
        github_sync.message,
        f"GitHub remote files: {github_sync.remote_files}",
        f"GitHub files written locally: {github_sync.written_files}",
        f"Local files removed: {github_sync.removed_files}",
        f"Total available: {len(names)}",
        f"Added: {stats['added']}",
        f"Paths updated: {stats['updated']}",
        f"Removed: {stats['removed']}",
        f"file_id reset: {stats['file_ids_reset']}",
    ]

    if names:
        lines.append("")
        lines.append("Available emoji:")
        lines.append(", ".join(names))
    else:
        lines.append("")
        lines.append("The emoji folder is currently empty.")

    await message.answer("\n".join(lines))


@router.message(Command("clear"))
async def clear_command(message: Message):
    github_sync = sync_emoji_dir_with_github()
    clear_stats = clear_emoji_state()
    emoji_map, sync_stats = sync_emoji_db(reset_file_ids=True)
    names = sorted(emoji_map)

    lines = [
        "Database fully rebuilt.",
        github_sync.message,
        f"GitHub remote files: {github_sync.remote_files}",
        f"GitHub files written locally: {github_sync.written_files}",
        f"Local files removed: {github_sync.removed_files}",
        f"Rows removed from DB: {clear_stats['db_rows_removed']}",
        f"Converted files removed: {clear_stats['converted_removed']}",
        f"Inserted from emoji folder: {sync_stats['added']}",
        f"Total available: {len(names)}",
    ]

    if names:
        lines.append("")
        lines.append("Available emoji:")
        lines.append(", ".join(names))
    else:
        lines.append("")
        lines.append("The emoji folder is currently empty.")

    await message.answer("\n".join(lines))


@router.message(F.text)
async def handle_text(message: Message):
    emoji_map, _ = sync_emoji_db()
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
            logger.warning("File %s is missing on disk, syncing database", file_path)
            sync_emoji_db()
            continue

        try:
            prepared_media = prepare_media_for_sending(file_path)
        except RuntimeError as error:
            logger.exception("Could not prepare %s for sending", file_path.name)
            await message.answer(str(error))
            continue

        if record["telegram_file_id"]:
            cached_media_type = get_cached_media_type(record, file_path)
            logger.info("Sending %s via cached file_id", name)
            if cached_media_type == "animation":
                await message.answer_animation(record["telegram_file_id"])
            else:
                await message.answer_sticker(record["telegram_file_id"])
            continue

        logger.info("Sending %s from file %s", name, prepared_media.file_path)
        if prepared_media.media_type == "animation":
            sent_message = await message.answer_animation(
                FSInputFile(str(prepared_media.file_path))
            )
            telegram_file_id = sent_message.animation.file_id
        else:
            sent_message = await message.answer_sticker(
                sticker=FSInputFile(str(prepared_media.file_path))
            )
            telegram_file_id = sent_message.sticker.file_id

        save_telegram_file_id(name, telegram_file_id, prepared_media.media_type)


async def main():
    sync_emoji_db()
    logger.info("Starting bot...")
    logger.info("BOT_TOKEN: %s...", TOKEN[:5])
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program stopped.")
    finally:
        conn.close()
