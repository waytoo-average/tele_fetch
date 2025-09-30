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
def fetch_folder_contents(folder_id):
    """
    Fetch all items from a folder, including pagination support.
    Returns a list of items with their metadata.
    """
    try:
        url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': f"'{folder_id}' in parents and trashed=false",
            'key': API_KEY,
            'pageSize': 1000,
            'fields': 'nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime)',
        }
        
        all_files = []
        while True:
            res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            data = res.json()
            
            all_files.extend(data.get("files", []))
            
            # Check for more pages
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params['pageToken'] = next_page_token
            time.sleep(0.1)  # Small delay between paginated requests
        
        return all_files
    
    except requests.RequestException as e:
        print(f"❌ Error fetching folder {folder_id}: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error fetching folder {folder_id}: {e}")
        return []

def is_folder(item):
    """Check if an item is a folder based on its mimeType."""
    return item.get("mimeType") == "application/vnd.google-apps.folder"

def get_all_files_recursively(folder_id, visited=None):
    """
    Recursively traverse all subfolders and return only files (not folders).
    Uses visited set to prevent infinite loops in case of circular references.
    """
    if visited is None:
        visited = set()
    
    # Prevent infinite loops
    if folder_id in visited:
        return []
    visited.add(folder_id)
    
    all_files = []
    items = fetch_folder_contents(folder_id)
    
    for item in items:
        if is_folder(item):
            # It's a folder, recurse into it
            print(f"  📁 Entering subfolder: {item['name']}")
            subfolder_files = get_all_files_recursively(item['id'], visited)
            all_files.extend(subfolder_files)
        else:
            # It's a file, add it to our list
            all_files.append(item)
    
    return all_files

def save_to_rtdb(topic, file):
    """Save file metadata to Firebase Realtime Database."""
    try:
        file_id = file["id"]
        ref = rtdb.reference(f"updates/{topic}/{file_id}")
        
        if ref.get() is None:
            ref.set({
                "name": file["name"],
                "link": f"https://drive.google.com/file/d/{file_id}/view?usp=sharing",
                "topic": topic,
                "description": "",
                "id": file_id,
                "mimeType": file.get("mimeType", ""),
                "size": file.get("size", ""),
                "createdTime": file.get("createdTime", ""),
                "modifiedTime": file.get("modifiedTime", ""),
                "timestamp": int(time.time())
            })
            print(f"🟢 RTDB → Saved: {file['name']}")
            return True
        return False
    
    except Exception as e:
        print(f"❌ Error saving to RTDB: {e}")
        return False

def save_to_firestore(topic, file):
    """Save file metadata to Firestore."""
    try:
        file_id = file["id"]
        doc_ref = firestore_db.collection("updates").document(topic).collection("files").document(file_id)
        
        if not doc_ref.get().exists:
            doc_ref.set({
                "name": file["name"],
                "link": f"https://drive.google.com/file/d/{file_id}/view?usp=sharing",
                "topic": topic,
                "description": "",
                "id": file_id,
                "mimeType": file.get("mimeType", ""),
                "size": file.get("size", ""),
                "createdTime": file.get("createdTime", ""),
                "modifiedTime": file.get("modifiedTime", ""),
                "timestamp": int(time.time())
            })
            print(f"🟣 Firestore → Saved: {file['name']}")
            return True
        return False
    
    except Exception as e:
        print(f"❌ Error saving to Firestore: {e}")
        return False

# ---------------------------
# 4. Main Loop
# ---------------------------
def main():
    """Main execution loop."""
    print("🚀 Starting Google Drive monitoring script...")
    print(f"📊 Monitoring {len(FOLDERS)} folders\n")
    
    total_new_files = 0
    
    for topic, folder_id in FOLDERS.items():
        print(f"\n{'='*60}")
        print(f"📂 Processing: {topic}")
        print(f"{'='*60}")
        
        try:
            # Get all files recursively (excluding folders)
            files = get_all_files_recursively(folder_id)
            print(f"📄 Found {len(files)} files (after recursively scanning subfolders)")
            
            new_files_count = 0
            for f in files:
                # Save to both DBs
                new_in_rtdb = save_to_rtdb(topic, f)
                new_in_fs = save_to_firestore(topic, f)
                
                if new_in_rtdb or new_in_fs:
                    print(f"✅ New file recorded: {f['name']}")
                    new_files_count += 1
                    total_new_files += 1
                
                # Small delay to avoid overwhelming the databases
                time.sleep(0.05)
            
            if new_files_count == 0:
                print(f"✓ No new files in {topic}")
            else:
                print(f"✓ Added {new_files_count} new files from {topic}")
        
        except Exception as e:
            print(f"❌ Error processing {topic}: {e}")
            continue
        
        # Delay between folders to respect rate limits
        time.sleep(0.5)
    
    print(f"\n{'='*60}")
    print(f"🎉 Script completed!")
    print(f"📊 Total new files added: {total_new_files}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Script interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
