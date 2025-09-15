import os
import asyncio
from telethon import TelegramClient
from supabase import create_client, Client

# --- Load environment variables ---
TG_API_ID = int(os.getenv("TG_API_ID") or 0)
TG_API_HASH = os.getenv("TG_API_HASH")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not TG_API_ID or not TG_API_HASH or not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Missing required environment variables!")

# --- Init clients ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = TelegramClient("anon", TG_API_ID, TG_API_HASH)

# --- Telegram channel username ---
CHANNEL = "studying_chinese_with_mazen"


async def fetch_and_store():
    # 1. Find the last saved message ID in Supabase
    result = supabase.table("tips").select("id").order("id", desc=True).limit(1).execute()
    last_id = result.data[0]["id"] if result.data else 0
    print(f"📌 Last stored ID: {last_id}")

    # 2. Connect to Telegram
    await client.start()

    # 3. Fetch only newer messages
    async for msg in client.iter_messages(CHANNEL, min_id=last_id):
        if not msg.text:  # skip empty / media messages
            continue

        supabase.table("tips").upsert({
            "id": msg.id,
            "content": msg.text,
            "date": msg.date.isoformat() if msg.date else None
        }).execute()

        print(f"✅ Stored message {msg.id}")


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(fetch_and_store())
