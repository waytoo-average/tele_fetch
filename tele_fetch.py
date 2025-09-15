import os
from telethon import TelegramClient
from supabase import create_client, Client

# --- Load credentials from env vars or fallback ---
TG_API_ID = int(os.getenv("TG_API_ID", "15561901"))
TG_API_HASH = os.getenv("TG_API_HASH", "b933d57ee634350cf829a12e8f75d2d4")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://uvtdyyutnvcjvylutqgc.supabase.co")
# ⚠️ Replace with your service_role key (not anon!)
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InV2dGR5eXV0bnZjanZ5bHV0cWdjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1Nzk1OTE3MSwiZXhwIjoyMDczNTM1MTcxfQ.xx1LYlp86QRT7Z-cVqPLHzFAvmuAcLC6AIdCsTzjrv8")

# Telegram channel
CHANNEL = "https://t.me/studying_chinese_with_mazen"

# Init clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = TelegramClient("tg_session", TG_API_ID, TG_API_HASH)

async def fetch_and_store():
    await client.start()
    entity = await client.get_entity(CHANNEL)

    # Get last stored ID
    last_id = 0
    res = supabase.table("metadata").select("value").eq("key", "last_tip_id").execute()
    if res.data:
        last_id = int(res.data[0]["value"])

    # Fetch messages newer than last_id
    async for msg in client.iter_messages(entity, min_id=last_id):
        if msg.text:
            # Insert into Supabase
            supabase.table("tips").insert({
                "id": msg.id,
                "text": msg.text,
                "date": str(msg.date)
            }).execute()
            print(f"Inserted message id {msg.id}")

            # Update metadata
            supabase.table("metadata").update({"value": str(msg.id)}).eq("key", "last_tip_id").execute()

    print("✅ Done syncing tips!")

with client:
    client.loop.run_until_complete(fetch_and_store())
