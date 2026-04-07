import os
from telethon import TelegramClient
from telethon import TelegramClient
import os
from dotenv import load_dotenv

load_dotenv()  # Мұны қосуды ұмытпа

TELETHON_API_ID = int(os.getenv("TELETHON_API_ID"))
TELETHON_API_HASH = os.getenv("TELETHON_API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "qazload")

client = TelegramClient(SESSION_NAME, TELETHON_API_ID, TELETHON_API_HASH)
                                               async def start_telethon():
    if not client.is_connected():
        await client.connect()                     if not await client.is_user_authorized():
        print("📱 Телетон аккаунтқа кіру қажет:")
        await client.start()  # Бір рет логин енгізесің (код сұрайды)
    print("✅ Telethon сессия дайын!")         
async def send_big_file(chat_id: int, file_path: str, caption: str = None):
    """
    Үлкен файлдарды Telethon арқылы жібереді.
    """
    await client.send_file(chat_id, file_path, caption=caption)