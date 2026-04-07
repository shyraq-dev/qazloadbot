import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = YOUR_API_ID       # ← өз API_ID енгіз    API_HASH = "YOUR_API_HASH"   # ← өз API_HASH енгіз
SESSION = "Your_StringSession"  # ← өз StringSession енгіз
                                               async def main():
    print("🔍 Telethon тексеріліп жатыр...")

    try:
        client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)                             await client.connect()
                                                       if not await client.is_user_authorized():
            print("❌ Telethon: Авторизация ЖОҚ!")
            print("➡ Алдымен StringSession алып келу керек.")
            return

        me = await client.get_me()
        print("✅ Telethon қосулы!")                   print(f"👤 Қолданушы: {me.first_name} (ID: {me.id})")

        # файл жіберуді да тексеріп көрейік:           try:
            print("📤 Тест жіберіліп жатыр...")
            await client.send_message("me", "Telethon тест: бәрі жұмыс істеп тұр!")
            print("✅ Telegram-ға жіберілді!")
        except Exception as e:
            print("⚠️ Жіберу қатесі:", e)

        await client.disconnect()

    except Exception as e:
        print("❌ Telethon мүлде қосылмай жатыр!")
        print("Қате:", e)

asyncio.run(main())