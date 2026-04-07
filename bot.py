#!/usr/bin/env python3
"""
QazLoad Bot — СОҢҒЫ НҰСҚА (Telethon entity дұрыс өңдеумен)
"""

import asyncio
import logging
import os
import shutil
import tempfile
import html
import re
import uuid
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from enum import Enum

from dotenv import load_dotenv

import aiosqlite
from yt_dlp import YoutubeDL, DownloadError
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, CallbackQuery, FSInputFile, ErrorEvent
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramBadRequest,
    TelegramNotFound,
    TelegramRetryAfter,
    RestartingTelegram,
)
from telethon import TelegramClient
from telethon.tl.types import PeerUser, PeerChat, PeerChannel, DocumentAttributeVideo
from telethon.errors import UserNotParticipantError

load_dotenv()

# ---------- Config ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN") or ""
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN анықталмаған — қызметті тоқтатамын.")
    raise SystemExit("BOT_TOKEN анықталмады. .env файлын тексер.")

TELETHON_API_ID = int(os.getenv("TELETHON_API_ID") or 0)
TELETHON_API_HASH = os.getenv("TELETHON_API_HASH") or ""
TELETHON_SESSION = os.getenv("TELETHON_SESSION") or "telethon/qazload.session"

DB_PATH = os.getenv("DB_PATH") or "data/qazload.db"
CONCURRENT_DOWNLOADS = int(os.getenv("CONCURRENT_DOWNLOADS") or 2)
MAX_LINKS_PER_MESSAGE = int(os.getenv("MAX_LINKS_PER_MESSAGE") or 5)

TELEGRAM_UPLOAD_THRESHOLD = int(os.getenv("TELEGRAM_UPLOAD_THRESHOLD") or 45 * 1024 * 1024)

URL_RE = re.compile(r"https?://[^\s'\"<>)\]]+", re.IGNORECASE)

# ---------- Quality Enum ----------
class Quality(str, Enum):
    P144 = "144"
    P240 = "240"
    P360 = "360"
    P480 = "480"
    P720 = "720"
    P1080 = "1080"
    P1440 = "1440"
    P2160 = "2160"
    P4320 = "4320"
    AUDIO = "audio"
    AUTO = "auto"
    PREVIEW = "preview"

QUALITY_OPTIONS = [q.value for q in Quality]

# ---------- App setup ----------
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

download_semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
telethon_client: Optional[TelegramClient] = None
PENDING_JOBS: Dict[str, Dict[str, Any]] = {}
pending_jobs_lock = asyncio.Lock()

# ---------- Config Validation ----------
def validate_config():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN жоқ")
    if TELEGRAM_UPLOAD_THRESHOLD < 1024:
        logger.warning(f"TELEGRAM_UPLOAD_THRESHOLD тым кіші: {TELEGRAM_UPLOAD_THRESHOLD}")
    if TELETHON_API_ID and not TELETHON_API_HASH:
        logger.warning("TELETHON_API_ID бар, бірақ TELETHON_API_HASH жоқ!")
    logger.info(f"✅ Config: THRESHOLD={human_size(TELEGRAM_UPLOAD_THRESHOLD)}, CONCURRENT={CONCURRENT_DOWNLOADS}")

# ---------- DB helpers ----------
async def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                url TEXT,
                filename TEXT,
                size INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

        try:
            async with db.execute("PRAGMA table_info(downloads)") as cursor:
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]

            if 'user_id' not in column_names:
                logger.info("🔧 DB Migration v2: user_id бағанын қосу...")
                await db.execute("ALTER TABLE downloads ADD COLUMN user_id TEXT")
                await db.commit()
                logger.info("✅ Migration v2 сәтті: user_id қосылды")
            else:
                logger.debug("✅ DB схемасы жаңа нұсқада (user_id бар)")
        except Exception as e:
            logger.exception("❌ Migration қатесі: %s", e)

    logger.info("✅ Database дайын: %s", DB_PATH)

async def save_download_record(chat_id: str, user_id: str, url: str, filename: str, size: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO downloads (chat_id, user_id, url, filename, size) VALUES (?, ?, ?, ?, ?)",
                (chat_id, user_id, url, filename, size),
            )
            await db.commit()
        logger.debug("DB: %s сақталды (%s)", filename, human_size(size))
    except Exception as e:
        logger.exception("DB сақтау қатесі: %s - %s", url, e)

# ---------- Utilities ----------
def find_links(text: str) -> List[str]:
    if not text:
        return []
    raw = URL_RE.findall(text)
    cleaned = []
    for u in raw:
        u = u.rstrip(".,;:!?'\")]}")
        if u and len(u) > 10:
            cleaned.append(u)
    return cleaned

def human_size(num_bytes: int) -> str:
    num = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"

def make_progress_bar(percent: float, length: int = 12) -> str:
    filled = max(0, min(length, int(length * percent / 100)))
    return "▰" * filled + "▱" * (length - filled)

def make_progress_hook(chat_id: str, msg_id: int, loop: asyncio.AbstractEventLoop):
    last_text = {"value": ""}
    last_update = {"time": 0.0}

    async def _edit(text: str):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                logger.debug("Progress edit қатесі: %s", e)
        except TelegramForbiddenError:
            logger.warning("User blocked bot: %s", chat_id)
        except Exception as e:
            logger.debug("Progress edit қатесі: %s", e)

    def hook(d):
        status = d.get("status")
        if status not in ("downloading", "finished"):
            return

        current_time = time.time()
        if status == "downloading" and (current_time - last_update["time"] < 1.5):
            return
        last_update["time"] = current_time

        downloaded = d.get("downloaded_bytes") or 0
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        speed = d.get("speed") or 0
        eta = d.get("eta") or 0
        percent = (downloaded / total * 100) if total else 0.0

        if status == "downloading":
            bar = make_progress_bar(percent)
            txt = (f"🔽 <b>Жүктеліп жатыр:</b> {percent:.1f}%\n{bar}\n"
                   f"{human_size(int(downloaded))} / {human_size(int(total))}\n"
                   f"⚡ {human_size(int(speed))}/s | ⏱ {int(eta)}s қалды")
        else:
            txt = "✅ Жүктеу аяқталды, файл өңделуде..."

        if txt != last_text["value"]:
            last_text["value"] = txt
            try:
                asyncio.run_coroutine_threadsafe(_edit(txt), loop)
            except Exception as e:
                logger.debug("asyncio.run_coroutine_threadsafe қатесі: %s", e)

    return hook

# ---------- Safe Message Send ----------
async def safe_send_message(chat_id: str, text: str, **kwargs):
    try:
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except TelegramForbiddenError:
        logger.warning("⚠️ User blocked bot: %s", chat_id)
        return None
    except TelegramBadRequest as e:
        logger.error("❌ Bad request: %s - %s", chat_id, e)
        return None
    except TelegramRetryAfter as e:
        logger.warning("⏳ Rate limit: waiting %s seconds", e.retry_after)
        await asyncio.sleep(e.retry_after)
        return await safe_send_message(chat_id, text, **kwargs)
    except Exception as e:
        logger.exception("❌ Send message error: %s", e)
        return None

async def safe_edit_message(chat_id: str, message_id: int, text: str, **kwargs):
    try:
        return await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, **kwargs)
    except TelegramForbiddenError:
        logger.warning("⚠️ User blocked bot: %s", chat_id)
        return None
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.debug("Edit message error: %s", e)
        return None
    except TelegramRetryAfter as e:
        logger.warning("⏳ Rate limit: waiting %s seconds", e.retry_after)
        await asyncio.sleep(e.retry_after)
        return await safe_edit_message(chat_id, message_id, text, **kwargs)
    except Exception as e:
        logger.debug("Edit message error: %s", e)
        return None

# ---------- Keyboard ----------
def quality_keyboard(prefix: str):
    builder = InlineKeyboardBuilder()
    for q in QUALITY_OPTIONS:
        display = q + "p" if q.isdigit() else q
        builder.button(text=display, callback_data=f"{prefix}|{q}")
    builder.adjust(3)
    return builder.as_markup()

# ---------- YT-DLP helpers ----------
def build_ydl_opts(outtmpl: str, choice: str, cookies: Optional[str], progress_hook=None):
    common = {
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook] if progress_hook else [],
        "progress_interval": 0.5,
        "writesubtitles": False,
        "writethumbnail": True,
        "merge_output_format": "mp4",
        "rm_cachedir": True,
        "socket_timeout": 30,
        "retries": 3,
    }

    if cookies:
        common["cookiefile"] = cookies

    if choice == Quality.PREVIEW.value:
        opts = {**common, "skip_download": True, "writethumbnail": True}
        return opts

    if choice == Quality.AUDIO.value:
        opts = {
            **common,
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                },
            ],
        }
        return opts

    if choice == Quality.AUTO.value:
        opts = {
            **common,
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
            "merge_output_format": "mp4",
        }
        return opts

    if choice.isdigit():
        height = int(choice)
        opts = {
            **common,
            "format": f"bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<={height}]+bestaudio/best[height<={height}]",
            "merge_output_format": "mp4",
        }
        return opts

    logger.warning("Белгісіз сапа таңдауы: %s, auto қолданамын", choice)
    return {**common, "format": "best", "merge_output_format": "mp4"}

async def run_ydl_in_thread(opts: dict, url: str):
    def _run():
        with YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=(not opts.get("skip_download", False)))
    return await asyncio.to_thread(_run)

# ---------- Telethon ----------
async def init_telethon():
    global telethon_client
    if not TELETHON_API_ID or not TELETHON_API_HASH:
        logger.warning("⚠️ Telethon параметрлері жоқ — үлкен файлдар жіберілмейді.")
        return

    try:
        Path(TELETHON_SESSION).parent.mkdir(parents=True, exist_ok=True)
        telethon_client = TelegramClient(TELETHON_SESSION, TELETHON_API_ID, TELETHON_API_HASH)
        await telethon_client.start()
        me = await telethon_client.get_me()
        logger.info("✅ Telethon іске қосылды: @%s", me.username or me.id)
    except Exception as e:
        logger.exception("❌ Telethon іске қосылу қатесі: %s", e)
        telethon_client = None

async def close_telethon():
    global telethon_client
    if telethon_client:
        try:
            await telethon_client.disconnect()
            logger.info("✅ Telethon тоқтатылды.")
        except Exception as e:
            logger.exception("❌ Telethon тоқтату қатесі: %s", e)

async def check_telethon_user(user_id: int) -> bool:
    """
    🧠 КІЛТ ФУНКЦИЯ: Telethon user-ді тани ма?

    Returns:
        True: Tanymyn, jiberu mumkin ✅
        False: Tanymaymyn, jibeuge bolmayd ❌
    """
    if not telethon_client:
        return False

    try:
        # get_entity — "сені танимын ба?" сұрауы
        entity = await telethon_client.get_entity(user_id)
        logger.info("✅ Telethon: user %s ТАНЫЛДЫ (entity табылды)", user_id)
        return True
    except ValueError:
        # "Жоқ, танымаймын"
        logger.warning("❌ Telethon: user %s ТАНЫЛМАДЫ (entity жоқ)", user_id)
        return False
    except Exception as e:
        logger.exception("❌ Telethon get_entity қатесі: %s", e)
        return False

async def send_file_telethon(user_id: int, file_path: Path, caption: Optional[str] = None):
    """
    Telethon арқылы файл жіберу (ҚАРАПАЙЫМ НҰСҚА)

    ШАРТ: check_telethon_user() МІНДЕТТІ шақырылуы керек!
    """
    if not telethon_client:
        raise RuntimeError("Telethon клиенті іске қосылмаған.")

    try:
        file_size = file_path.stat().st_size

        if file_size > 2 * 1024 * 1024 * 1024:
            raise ValueError(f"Файл тым үлкен: {human_size(file_size)} (max 2GB)")

        logger.info("📤 Telethon upload: %s (%s) -> user %s", file_path.name, human_size(file_size), user_id)

        kwargs = {
            "entity": user_id,  # INT user_id — ең қарапайым
            "file": str(file_path),
            "caption": caption or f"📥 {file_path.name}\n@QazLoadBot",
            "force_document": False,
            "parse_mode": None,
            "supports_streaming": True,
        }

        if file_size > 10 * 1024 * 1024:
            ext = file_path.suffix.lower()
            if ext in ('.mp4', '.mkv', '.webm', '.mov', '.avi'):
                kwargs["attributes"] = [
                    DocumentAttributeVideo(
                        duration=0,
                        w=1920,
                        h=1080,
                        supports_streaming=True
                    )
                ]

        await telethon_client.send_file(**kwargs)
        logger.info("✅ Telethon жіберілді: %s -> user %s", file_path.name, user_id)

    except Exception as e:
        logger.exception("❌ Telethon жіберу қатесі: %s", e)
        raise

# ---------- Core functions ----------
async def download_and_send(chat_id: str, status_msg_id: int, url: str, choice: str, user_id: Optional[int] = None, cookies: Optional[str] = None):
    chat_id_str = str(chat_id)
    loop = asyncio.get_running_loop()

    async with download_semaphore:
        tmpdir = Path(tempfile.mkdtemp(prefix="qazload_"))
        logger.info("📁 Temp dir: %s", tmpdir)

        try:
            outtmpl = str(tmpdir / "%(title).200s.%(ext)s")
            hook = make_progress_hook(chat_id_str, status_msg_id, loop)
            ydl_opts = build_ydl_opts(outtmpl, choice=choice, cookies=cookies, progress_hook=hook)

            try:
                logger.info("🔽 Жүктеу басталды: %s (сапа: %s, user: %s)", url, choice, user_id)
                info = await run_ydl_in_thread(ydl_opts, url)
            except DownloadError as e:
                err = html.escape(str(e))
                logger.error("❌ YT-DLP DownloadError: %s - %s", url, e)
                await safe_edit_message(
                    chat_id_str,
                    status_msg_id,
                    f"❌ Жүктеу қатесі:\n<code>{err}</code>\n\nСілтемені тексер немесе басқа сапа таңда."
                )
                return
            except Exception as e:
                logger.exception("❌ YT-DLP жалпы қате: %s", url)
                err = html.escape(str(e))
                await safe_edit_message(
                    chat_id_str,
                    status_msg_id,
                    f"❌ Жүктеу қатесі:\n<code>{err}</code>"
                )
                return

            if choice == Quality.PREVIEW.value:
                await handle_preview(tmpdir, chat_id_str, status_msg_id, url, info)
                return

            files = [f for f in tmpdir.iterdir() if f.is_file()]
            if not files:
                logger.warning("❌ Файл табылмады: %s", tmpdir)
                await safe_edit_message(
                    chat_id_str,
                    status_msg_id,
                    "❌ Файл табылмады. Қайта көріңіз."
                )
                return

            file_path = select_best_file(files, choice)
            size = file_path.stat().st_size
            logger.info("📦 Таңдалған файл: %s (%s)", file_path.name, human_size(size))

            try:
                asyncio.create_task(save_download_record(
                    chat_id_str,
                    str(user_id) if user_id else "unknown",
                    url,
                    file_path.name,
                    size
                ))
            except Exception as e:
                logger.exception("DB жазу қатесі: %s", e)

            if size > TELEGRAM_UPLOAD_THRESHOLD:
                await send_via_telethon(chat_id_str, status_msg_id, file_path, size, user_id=user_id)
            else:
                await send_via_bot_api(chat_id_str, status_msg_id, file_path, size)

        except Exception as e:
            logger.exception("❌ download_and_send жалпы қате: %s", e)
            await safe_edit_message(
                chat_id_str,
                status_msg_id,
                f"❌ Қате орын алды:\n<code>{html.escape(str(e))}</code>"
            )
        finally:
            try:
                shutil.rmtree(tmpdir, ignore_errors=False)
                logger.debug("🗑 Temp dir тазаланды: %s", tmpdir)
            except Exception as e:
                logger.warning("⚠️ Temp dir тазалау қатесі: %s - %s", tmpdir, e)

async def handle_preview(tmpdir: Path, chat_id_str: str, status_msg_id: int, url: str, info: dict):
    thumbs = [f for f in tmpdir.iterdir() if f.suffix.lower() in ('.jpg', '.jpeg', '.png', '.webp', '.gif')]

    if not thumbs:
        thumbnail_url = (info or {}).get("thumbnail")
        if thumbnail_url:
            logger.info("🖼 Thumbnail URL табылды: %s", thumbnail_url)
            try:
                thumb_opts = {
                    "outtmpl": str(tmpdir / "%(id)s.%(ext)s"),
                    "writethumbnail": True,
                    "skip_download": True,
                    "quiet": True,
                }
                await run_ydl_in_thread(thumb_opts, url)
                thumbs = [f for f in tmpdir.iterdir() if f.suffix.lower() in ('.jpg', '.jpeg', '.png', '.webp', '.gif')]
            except Exception as e:
                logger.exception("❌ Thumbnail жүктеу қатесі: %s", e)

    if thumbs:
        thumb = max(thumbs, key=lambda f: f.stat().st_size)
        logger.info("✅ Thumbnail табылды: %s", thumb.name)
        await safe_edit_message(chat_id_str, status_msg_id, "🖼 Thumbnail дайын, жіберілуде...")

        try:
            fobj = FSInputFile(str(thumb), filename=thumb.name)
            sent = await bot.send_photo(
                chat_id=chat_id_str,
                photo=fobj,
                caption=f"🖼 {thumb.name}\nThumbnail — @QazLoadBot"
            )
            if sent:
                await safe_edit_message(chat_id_str, status_msg_id, "✅ Thumbnail жіберілді.")
        except TelegramForbiddenError:
            logger.warning("⚠️ User blocked bot during thumbnail send: %s", chat_id_str)
        except Exception as e:
            logger.exception("❌ Telegram-ға thumbnail жіберу қатесі: %s", e)
            await safe_edit_message(
                chat_id_str,
                status_msg_id,
                f"❌ Thumbnail жіберу қатесі:\n<code>{html.escape(str(e))}</code>"
            )
    else:
        logger.warning("❌ Thumbnail файл табылмады")
        await safe_edit_message(chat_id_str, status_msg_id, "❌ Thumbnail табылмады немесе қол жетімді емес.")

def select_best_file(files: List[Path], choice: str) -> Path:
    video_exts = ('.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv')
    audio_exts = ('.mp3', '.m4a', '.wav', '.flac', '.ogg', '.aac')

    video_files = [f for f in files if f.suffix.lower() in video_exts]
    audio_files = [f for f in files if f.suffix.lower() in audio_exts]
    other_files = [f for f in files if f not in video_files + audio_files]

    if choice == Quality.AUDIO.value:
        if audio_files:
            return max(audio_files, key=lambda f: f.stat().st_size)
        elif other_files:
            return max(other_files, key=lambda f: f.stat().st_size)
        elif video_files:
            return max(video_files, key=lambda f: f.stat().st_size)

    mp4_files = [f for f in video_files if f.suffix.lower() == '.mp4']
    if mp4_files:
        return max(mp4_files, key=lambda f: f.stat().st_size)
    elif video_files:
        return max(video_files, key=lambda f: f.stat().st_size)
    elif audio_files:
        return max(audio_files, key=lambda f: f.stat().st_size)
    elif other_files:
        return max(other_files, key=lambda f: f.stat().st_size)

    return max(files, key=lambda f: f.stat().st_size)

async def send_via_bot_api(chat_id_str: str, status_msg_id: int, file_path: Path, size: int):
    await safe_edit_message(
        chat_id_str,
        status_msg_id,
        f"📤 {file_path.name} ({human_size(size)}) — Telegram арқылы жіберілуде..."
    )

    try:
        fobj = FSInputFile(str(file_path), filename=file_path.name)
        ext = file_path.suffix.lower()
        caption_text = f"📥 <b>{file_path.name}</b>\n{human_size(size)} — @QazLoadBot"

        sent = None
        if ext in ('.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv'):
            sent = await bot.send_video(chat_id=chat_id_str, video=fobj, caption=caption_text, supports_streaming=True)
        elif ext in ('.mp3', '.m4a', '.wav', '.flac', '.ogg', '.aac'):
            sent = await bot.send_audio(chat_id=chat_id_str, audio=fobj, caption=caption_text)
        elif ext in ('.jpg', '.jpeg', '.png', '.webp', '.gif'):                                           sent = await bot.send_photo(chat_id=chat_id_str, photo=fobj, caption=caption_text)        else:                                              sent = await bot.send_document(chat_id=chat_id_str, document=fobj, caption=caption_text)
                                                       if sent:
            logger.info("✅ Bot API: %s жіберілді", file_path.name)
            await safe_edit_message(                           chat_id_str,
                status_msg_id,
                f"✅ <b>Жіберілді:</b> {file_path.name} ({human_size(size)})"
            )

    except TelegramForbiddenError:
        logger.warning("⚠️ User blocked bot during file send: %s", chat_id_str)                    except Exception as e:
        logger.exception("❌ Bot API жіберу қатесі: %s", e)
        err = html.escape(str(e))
        await safe_edit_message(
            chat_id_str,                                   status_msg_id,
            f"❌ Telegram жіберу қатесі:\n<code>{err}</code>\n\nФайл тым үлкен болуы мүмкін."
        )

async def send_via_telethon(chat_id_str: str, status_msg_id: int, file_path: Path, size: int, user_id: Optional[int] = None):
    """
    🎯 НЕГІЗГІ ЛОГИКА: Тексеру → Жіберу немесе Нұсқау
    """
    if not telethon_client:
        msg = (
            f"❗ <b>Файл тым үлкен:</b> {file_path.name} ({human_size(size)})\n\n"
            "Telethon конфигурациясы жоқ. Әкімшіден сұра немесе кішірек сапа таңда.\n"
            f"Шек: {human_size(TELEGRAM_UPLOAD_THRESHOLD)}"                                           )
        logger.warning("⚠️ Telethon жоқ, үлкен файл жіберілмейді: %s", file_path.name)                 await safe_edit_message(chat_id_str, status_msg_id, msg)
        return
                                                   MAX_TELEGRAM_FILE_SIZE = 2 * 1024 * 1024 * 1024
    if size > MAX_TELEGRAM_FILE_SIZE:                  msg = (                                            f"❗ <b>Файл Telegram шегінен асып кетті:</b>\n"
            f"{file_path.name} ({human_size(size)})\n\n"
            f"Telegram максимум шегі: {human_size(MAX_TELEGRAM_FILE_SIZE)}\n\n"
            "Кішірек сапа таңда (720p, 480p, 360p)"
        )
        logger.warning("⚠️ Файл тым үлкен (>2GB): %s", file_path.name)
        await safe_edit_message(chat_id_str, status_msg_id, msg)
        return

    if not user_id:
        msg = (                                            f"❌ <b>Қате:</b> user_id табылмады\n\n"                                                      f"Файл: {file_path.name} ({human_size(size)})\n"                                              "Бұл техникалық қате, қайта көріңіз."
        )
        logger.error("❌ user_id жоқ, файл жіберілмейді")
        await safe_edit_message(chat_id_str, status_msg_id, msg)                                      return
                                                   # 🔥 КІЛТ: Telethon user-ді тани ма?
    can_send = await check_telethon_user(user_id)

    if not can_send:                                   # ❌ ТАНЫМАЙДЫ → Нұсқау беру
        msg = (                                            f"📬 <b>Үлкен файл дайын:</b> {file_path.name} ({human_size(size)})\n\n"
            f"❗ <b>Файлды алу үшін:</b>\n"                f"1️⃣ @QazLoadBot-қа жеке чатта кіріп\n"
            f"2️⃣ /start командасын жібер\n"
            f"3️⃣ Содан кейін файл автоматты түрде жіберіледі\n\n"
            f"💡 <b>Неліктен?</b>\n"                       f"Үлкен файлдар (>{human_size(TELEGRAM_UPLOAD_THRESHOLD)}) жеке чат арқылы жіберіледі.\n"
            f"Бірақ сенімен әлі чат ашылмаған  🔒\n\n"
            f"👉 Қазір @QazLoadBot-қа кіріп /start жіберіңіз!"
        )                                              logger.warning("⚠️ User %s танылмады — нұсқау жіберілді", user_id)                             await safe_edit_message(chat_id_str, status_msg_id, msg)                                      return
                                                   # ✅ ТАНЫЛДЫ → Файл жіберу
    await safe_edit_message(                           chat_id_str,
        status_msg_id,                                 f"⏫ {file_path.name} ({human_size(size)}) — Telethon арқылы жіберілуде...\n"
        f"⚠️ Үлкен файл, біраз уақыт алуы мүмкін.\n\n"
        f"📬 Файл сенің жеке чатыңа жіберіледі (@QazLoadBot)."
    )

    try:                                               caption = f"📥 {file_path.name}\n{human_size(size)} — @QazLoadBot"
        await send_file_telethon(user_id=user_id, file_path=file_path, caption=caption)

        logger.info("✅ Telethon: %s жіберілді -> user %s", file_path.name, user_id)
        await safe_edit_message(                           chat_id_str,
            status_msg_id,
            f"✅ <b>Telethon арқылы жіберілді:</b>\n{file_path.name} ({human_size(size)})\n\n"
            f"📬 Файл @QazLoadBot жеке чатыңа жіберілді."
        )

    except Exception as e:                             logger.exception("❌ Telethon жіберу қатесі: %s", e)
        err = html.escape(str(e))                      await safe_edit_message(
            chat_id_str,
            status_msg_id,
            f"❌ Telethon жіберу қатесі:\n<code>{err}</code>\n\n"
            f"Файл өлшемі: {human_size(size)}\n"                                                          "Кішірек сапа таңда немесе қайта көріңіз."
        )
                                               # ---------- Handlers ----------
@router.message(Command(commands=["start", "help"]))
async def cmd_start(message: Message):
    help_text = (
        "✨ <b>Сәлем, бұл QazLoad Bot!</b>\n\n"        "📎 <b>Қалай қолдану:</b>\n"
        "1. Маған әлеуметтік желіден сілтемені жібер\n"
        "2. Сапа таңда (144p-4320p, audio, auto, preview)\n"
        "3. Файлды күте тұр!\n\n"
        f"📊 <b>Шектер:</b>\n"
        f"• Бір хабарламада: {MAX_LINKS_PER_MESSAGE} сілтеме\n"
        f"• Bot API шегі: {human_size(TELEGRAM_UPLOAD_THRESHOLD)}\n"
        f"• Telethon: {'✅ Қосулы' if telethon_client else '❌ Өшірулі'}\n"
        f"• Максимум файл: 2GB (Telegram шегі)\n\n"
        "💡 <b>Сапа опциялары:</b>\n"                  "• <code>144p-4320p</code>: Видео сапасы\n"
        "• <code>audio</code>: Тек аудио (MP3)\n"
        "• <code>auto</code>: Ең жақсы сапа\n"         "• <code>preview</code>: Тек thumbnail\n\n"
        "🌐 <b>Қолдайтын платформалар:</b>\n"          "YouTube, TikTok, Instagram, Facebook, Twitter/X, Vimeo,\n"
        "VK, Rutube, Bilibili және 1000+ сайт\n\n"
        "📬 <b>Үлкен файлдар:</b>\n"
        f">{human_size(TELEGRAM_UPLOAD_THRESHOLD)} файлдар ТІКЕЛЕЙ сенің жеке чатыңа жіберіледі.\n"
        f"Бірінші рет пайдаланғанда бұл чатта /start жіберіңіз!\n\n"
        "🤖 <code>@QazLoadBot</code> — yt-dlp қуатымен"
    )                                          
    sent = await safe_send_message(str(message.chat.id), help_text)                               if not sent:                                       logger.warning("⚠️ Could not send start message to %s (probably blocked)", message.chat.id)
                                               @router.message()
async def handle_message(message: Message):
    text = (message.text or "") + " " + (message.caption or "")
    links = find_links(text)                                                                      if not links:                                      await safe_send_message(                           str(message.chat.id),
            "❌ Мәтінде сілтеме табылмады.\n\n"            "Мысалы: <code>https://youtube.com/watch?v=...</code>",                                       reply_to_message_id=message.message_id
        )                                              return                                 
    if len(links) > MAX_LINKS_PER_MESSAGE:
        await safe_send_message(
            str(message.chat.id),
            f"⚠️ Бір хабарламада тек {MAX_LINKS_PER_MESSAGE} сілтеме өңделеді.\n"
            f"Алғашқы {MAX_LINKS_PER_MESSAGE} сілтеме қабылданды.",
            reply_to_message_id=message.message_id
        )
        links = links[:MAX_LINKS_PER_MESSAGE]
                                                   user_id = message.from_user.id if message.from_user else None                                                                                for idx, url in enumerate(links, 1):
        prompt = await safe_send_message(
            str(message.chat.id),                          f"🔗 <b>Сілтеме {idx}/{len(links)}:</b>\n"
            f"<code>{html.escape(url)}</code>\n\n"                                                        "👇 Сапаны таңда:",                            reply_to_message_id=message.message_id                                                    )

        if not prompt:
            continue                           
        short_id = uuid.uuid4().hex[:12]               async with pending_jobs_lock:                      PENDING_JOBS[short_id] = {
                "url": url,
                "chat_id": str(message.chat.id),
                "user_id": user_id,                            "timestamp": time.time()                   }

        kb = quality_keyboard(short_id)                await safe_edit_message(
            str(message.chat.id),                          prompt.message_id,                             f"🔗 <b>Сілтеме {idx}/{len(links)}:</b>\n"
            f"<code>{html.escape(url)}</code>\n\n"                                                        "👇 Сапаны таңда:",
            reply_markup=kb
        )                                                                                             if len(links) > 1 and idx < len(links):            await asyncio.sleep(0.5)           
@router.callback_query()
async def process_quality(callback: CallbackQuery):                                               data = callback.data or ""
    if "|" not in data:                                await callback.answer("❌ Қате callback")
        return                                                                                    short_id, choice = data.split("|", 1)      
    async with pending_jobs_lock:
        job = PENDING_JOBS.pop(short_id, None)
                                                   if not job:
        await callback.answer("❌ Жұмыс табылмады немесе жойылған.", show_alert=True)                 return                                                                                    url = job["url"]
    chat_id = job["chat_id"]                       user_id = job.get("user_id")                                                                  if not user_id and callback.from_user:             user_id = callback.from_user.id                logger.info("👤 User ID callback-тан алынды: %s", user_id)

    logger.info("👤 User ID: %s, Chat ID: %s", user_id, chat_id)

    await callback.answer(f"✅ {choice} таңдалды, жүктеу басталды...")

    status_msg = await safe_send_message(
        chat_id=chat_id,
        text=f"⏳ <b>Жүктеу басталды:</b>\n"
             f"📎 Сілтеме: <code>{html.escape(url[:50])}...</code>\n"
             f"🎯 Сапа: <b>{choice}</b>"
    )

    if not status_msg:
        logger.warning("⚠️ Cannot send status message (user blocked): %s", chat_id)
        return

    asyncio.create_task(download_and_send(
        chat_id=chat_id,
        status_msg_id=status_msg.message_id,
        url=url,
        choice=choice,
        user_id=user_id
    ))

# ---------- Error Handler ----------
@dp.error()
async def error_handler(event: ErrorEvent):
    logger.exception("❌ Unhandled error: %s", event.exception)

    if isinstance(event.exception, TelegramForbiddenError):
        logger.warning("⚠️ Bot was blocked by user")
        return True

    if isinstance(event.exception, TelegramRetryAfter):
        logger.warning("⏳ Rate limit hit: waiting %s seconds", event.exception.retry_after)
        await asyncio.sleep(event.exception.retry_after)
        return True
                                                   if isinstance(event.exception, RestartingTelegram):
        logger.warning("⚠️ Telegram is restarting")
        await asyncio.sleep(5)                         return True

    return True

# ---------- Startup / Shutdown ----------
async def startup():
    logger.info("🚀 QazLoad Bot қосылуда...")
    validate_config()
    await init_db()
    await init_telethon()
    logger.info("✅ QazLoad Bot дайын!")

async def shutdown():
    logger.info("🛑 QazLoad Bot тоқтатылуда...")
    await close_telethon()
    try:                                               await bot.session.close()
    except Exception as e:
        logger.exception("Bot session жабу қатесі: %s", e)
    logger.info("👋 QazLoad Bot тоқтатылды.")

async def main():
    await startup()
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("⚠️ KeyboardInterrupt алынды")
    except Exception as e:                             logger.exception("❌ Polling қатесі: %s", e)
    finally:
        await shutdown()

if __name__ == "__main__":
    logging.getLogger("yt_dlp").setLevel(logging.WARNING)
    logging.getLogger("telethon").setLevel(logging.WARNING)

    try:                                               asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот тоқтатылды (Ctrl+C)")