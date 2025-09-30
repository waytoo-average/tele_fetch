import os
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, db, firestore

# ---------------------------
# 1. Config (from environment variables / secrets)
# ---------------------------
API_KEY = os.getenv("GOOGLE_API_KEY")

cred_json = json.loads(os.getenv("FIREBASE_SERVICE_ACCOUNT"))
cred = credentials.Certificate(cred_json)

firebase_admin.initialize_app(cred, {
    "databaseURL": os.getenv("FIREBASE_DB_URL")
})

rtdb = db
firestore_db = firestore.client()

# ---------------------------
# 2. Folders to Monitor
# ---------------------------
FOLDERS = {
    "First_Grade_Communication_S1": "1F-aqh6UK5x8Cbva6Zr0UvnchyV8hDGp2",
    "First_Grade_Communication_S2": "1Xl2tlH1leBqoY10nwZvt_Hs4qsCMSNeL",
    "First_Grade_Communication_LastYear_S1": "11YuvupTtPcZuTKAQOm9onpDrq6HwUTQS",
    "First_Grade_Communication_LastYear_S2": "12g0GqJ__VAOwwr9Skqy7DHhfAAumstxl",
    "First_Grade_Electronics_S1": "1PR7sqZwA_4LgumJC0k3Af0aZ_ba2Zl30",
    "First_Grade_Electronics_S2": "1N7EiilaFCKUEE_O7Dy3P71FAcuREylXo",
    "First_Grade_Mechatronics_S1": "1xthA_-KhZ5Edi3jloRkwRAIMM6ljpoIS",
    "First_Grade_Mechatronics_S2": "1rVnXee3-ELWNBJ8OSoOILw0DbmuKjGxo",
    "Second_Grade_Communication_S1": "1Ps1L5YOmU_LXfnb9sqwVtILVP5T3LjB9",
    "Second_Grade_Communication_S2": "1VUro5liVUNKtYG247Hwmq2fDQqkHqOhH",
    "Second_Grade_Communication_LastYear_S1": "1bm4KMv65KpJqFPLNFcSMf4DItNq-H0WS",
    "Second_Grade_Communication_LastYear_S2": "1rsfY18ebWzQPfYQIhhB_UAv0MIW_Q7Ot",
    "Second_Grade_Electronics_S1": "1q4FTG3ACwiu9z_n653kKkxx3DuZVJ2iV",
    "Second_Grade_Electronics_S2": "1XuYkkqZc1APLemfyKRuq_30Imzy_Hi4-",
    "Second_Grade_Mechatronics_S1": "1JKca-qZKuFN_S8wW0cNLYZqtJfuHyRJd",
    "Second_Grade_Mechatronics_S2": "1PL--naKlzPEehYLt4TbT9on4IG7szwmi",
    "Third_Grade_Communication_S1": "1LfORh3S9XzjmgeiPXpAHGs_78utNUznc",
    "Third_Grade_Communication_S2": "12edu3L3lWkiQTWqXkAWxzLaUo_4jXkXV",
    "Third_Grade_Communication_LastYear_S1": "1dnZ-B3w0eho4DLQuaUjcu_gYRxQVUh27",
    "Third_Grade_Communication_LastYear_S2": "1sHx6GHS5GGNfUWcAzJdDLXoxm5dJQUW5",
    "Third_Grade_Electronics_S1": "18s3_6cK3XaCGswmaaM3BqyqyWiE9adq0",
    "Third_Grade_Electronics_S2": "1g9QQto6aulAGb1s6jjBXStqnwyR3lWrI",
    "Third_Grade_Mechatronics_S1": "1pWHRg_DNnWHefQer6yfxf5SnNd4PFKVV",
    "Third_Grade_Mechatronics_S2": "16QAM_Dcbm9GPYOk5O5wi-Vo3WcS70Bus",
    "Fourth_Grade_Communication_S1": "1I353V2Dd1END87jCYcZb33fOmhjjKnGJ",
    "Fourth_Grade_Communication_S2": "1oJaBS3_nLYjCXIYOgqM5enjYxymw5h59",
    "Fourth_Grade_Communication_LastYear_S1": "1eTqTDJc6_u3EsgzJKjjYU6metlTP7kZu",
    "Fourth_Grade_Electronics_S1": "1KOX51U4QKDJ3plORY7c__YVh7j4A26SH",
    "Fourth_Grade_Electronics_S2": "11I6Q4nEoiXC6lxpo3vTEsalbblIqRIqU",
    "Fourth_Grade_Mechatronics_S1": "1x_uNebUvo3ZlqpciawuBnu0ooAg_pdfV",
    "Fourth_Grade_Mechatronics_S2": "1p5Y6tooBY9TVaz55mdYL7_xcJYzqY3nY",
}

# ---------------------------
# 3. Helpers
# ---------------------------
def fetch_folder_files(folder_id):
    url = f"https://www.googleapis.com/drive/v3/files?q='{folder_id}'+in+parents&key={API_KEY}"
    res = requests.get(url).json()
    return res.get("files", [])

def save_to_rtdb(topic, file):
    file_id = file["id"]
    ref = rtdb.reference(f"updates/{topic}/{file_id}")
    if ref.get() is None:
        ref.set({
            "name": file["name"],
            "link": f"https://drive.google.com/file/d/{file_id}/view?usp=sharing",
            "topic": topic,
            "description": "",
            "id": file_id,
            "timestamp": int(time.time())
        })
        print(f"🟢 RTDB → Saved: {file['name']}")
        return True
    return False

def save_to_firestore(topic, file):
    file_id = file["id"]
    doc_ref = firestore_db.collection("updates").document(topic).collection("files").document(file_id)
    if not doc_ref.get().exists:
        doc_ref.set({
            "name": file["name"],
            "link": f"https://drive.google.com/file/d/{file_id}/view?usp=sharing",
            "topic": topic,
            "description": "",
            "id": file_id,
            "timestamp": int(time.time())
        })
        print(f"🟣 Firestore → Saved: {file['name']}")
        return True
    return False

# ---------------------------
# 4. Main Loop
# ---------------------------
def main():
    for topic, folder_id in FOLDERS.items():
        files = fetch_folder_files(folder_id)
        for f in files:
            # Save to both DBs
            new_in_rtdb = save_to_rtdb(topic, f)
            new_in_fs = save_to_firestore(topic, f)
            if new_in_rtdb or new_in_fs:
                print(f"✅ New file recorded: {f['name']} in {topic}")

if __name__ == "__main__"
   


