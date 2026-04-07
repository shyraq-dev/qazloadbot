from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = your_API_ID_here        # Өз API ID енгіз
API_HASH = "your_API_HASH_here"   # Өз API HASH енгіз

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print("📱 Телефон нөміріңді енгіз:")
    client.start()
    print("✅ Жаңа STRING SESSION дайын:")
    print(client.session.save())