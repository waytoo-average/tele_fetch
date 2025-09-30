import os
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, messaging, db

# ---------------------------
# 1. Config
# ---------------------------
API_KEY = os.getenv("GOOGLE_API_KEY")

cred_json = json.loads(os.getenv("FIREBASE_SERVICE_ACCOUNT"))
cred = credentials.Certificate(cred_json)

firebase_admin.initialize_app(cred, {
    "databaseURL": os.getenv("FIREBASE_DB_URL")  # e.g. https://your-app.firebaseio.com
})

# Folders same as before (shortened here)
FOLDERS = {
    "First_Grade_Communication_S1": "1F-aqh6UK5x8Cbva6Zr0UvnchyV8hDGp2",
    # ...
}

# ---------------------------
# 2. Helpers
# ---------------------------
def fetch_folder_files(folder_id):
    url = f"https://www.googleapis.com/drive/v3/files?q='{folder_id}'+in+parents&key={API_KEY}"
    res = requests.get(url).json()
    return res.get("files", [])

def send_push(topic, title, body):
    message = messaging.Message(
        notification=messaging.Notification(title=title, body=body),
        topic=topic
    )
    response = messaging.send(message)
    print(f"✅ Sent notification to {topic}: {body}")

def save_to_rtdb(topic, file):
    file_id = file["id"]
    ref = db.reference(f"updates/{topic}/{file_id}")
    if ref.get() is None:  # prevent duplicates
        ref.set({
            "name": file["name"],
            "id": file_id,
            "timestamp": int(time.time()),
            "link": f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
        })
        print(f"📂 Saved to RTDB: {file['name']}")
        return True
    return False

# ---------------------------
# 3. Main logic
# ---------------------------
def main():
    for topic, folder_id in FOLDERS.items():
        files = fetch_folder_files(folder_id)
        for f in files:
            # If new file, save and notify
            if save_to_rtdb(topic, f):
                send_push(topic, "New Material Added", f["name"])

if __name__ == "__main__":
    main()
