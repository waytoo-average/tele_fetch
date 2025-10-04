import os
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, db, firestore
from datetime import datetime, timedelta

# Load .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, will use system environment variables

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
# 2. Mode Selection
# ---------------------------
# Set INITIAL_SYNC=true in GitHub Actions secrets for first run
# After that, remove it or set to false for fast incremental updates
INITIAL_SYNC = os.getenv("INITIAL_SYNC", "false").lower() == "true"

# For incremental syncs, only check files modified in last X minutes
# Set to slightly more than your cron interval (5 min cron = 10 min lookback for safety)
MINUTES_LOOKBACK = 10

# ---------------------------
# 3. Folders to Monitor
# ---------------------------
FOLDERS = {
    # First Grade
    "First_Grade_Communication_S1": "1F-aqh6UK5x8Cbva6Zr0UvnchyV8hDGp2",
    "First_Grade_Communication_S2": "1Xl2tlH1leBqoY10nwZvt_Hs4qsCMSNeL",
    "First_Grade_Communication_LastYear_S1": "11YuvupTtPcZuTKAQOm9onpDrq6HwUTQS",
    "First_Grade_Communication_LastYear_S2": "12g0GqJ__VAOwwr9Skqy7DHhfAAumstxl",
    "First_Grade_Electronics_S1": "1PR7sqZwA_4LgumJC0k3Af0aZ_ba2Zl30",
    "First_Grade_Electronics_S2": "1N7EiilaFCKUEE_O7Dy3P71FAcuREylXo",
    "First_Grade_Electronics_LastYear_S1": "11YuvupTtPcZuTKAQOm9onpDrq6HwUTQS",
    "First_Grade_Electronics_LastYear_S2": "1j65a7S832qfDMf1waGqW29zfjxofIXDs",
    "First_Grade_Mechatronics_S1": "1xthA_-KhZ5Edi3jloRkwRAIMM6ljpoIS",
    "First_Grade_Mechatronics_S2": "1rVnXee3-ELWNBJ8OSoOILw0DbmuKjGxo",
    "First_Grade_Mechatronics_LastYear_S1": "11YuvupTtPcZuTKAQOm9onpDrq6HwUTQS",
    "First_Grade_Mechatronics_LastYear_S2": "1--ivZFd-xXHTHuPrLsTVc7PelgptzvzA",
    
    # Second Grade
    "Second_Grade_Communication_S1": "1Ps1L5YOmU_LXfnb9sqwVtILVP5T3LjB9",
    "Second_Grade_Communication_S2": "1VUro5liVUNKtYG247Hwmq2fDQqkHqOhH",
    "Second_Grade_Communication_LastYear_S1": "1bm4KMv65KpJqFPLNFcSMf4DItNq-H0WS",
    "Second_Grade_Communication_LastYear_S2": "1rsfY18ebWzQPfYQIhhB_UAv0MIW_Q7Ot",
    "Second_Grade_Electronics_S1": "1q4FTG3ACwiu9z_n653kKkxx3DuZVJ2iV",
    "Second_Grade_Electronics_S2": "1XuYkkqZc1APLemfyKRuq_30Imzy_Hi4-",
    "Second_Grade_Electronics_LastYear_S1": "1kSU6_cE4mpUmX2WykGAQG3rvEmp1p9Ez",
    "Second_Grade_Electronics_LastYear_S2": "1LkNvv1ScdueAtl3FnjfcedrKc4rRMaJy",
    "Second_Grade_Mechatronics_S1": "1JKca-qZKuFN_S8wW0cNLYZqtJfuHyRJd",
    "Second_Grade_Mechatronics_S2": "1PL--naKlzPEehYLt4TbT9on4IG7szwmi",
    "Second_Grade_Mechatronics_LastYear_S1": "1BZmiGjnz5Pwktv1kZW8Zed8V-gZb_Ctt",
    "Second_Grade_Mechatronics_LastYear_S2": "1cjdOdSSji6Q2EdG9NdrkNOZzry5EXjvI",
    
    # Third Grade
    "Third_Grade_Communication_S1": "1LfORh3S9XzjmgeiPXpAHGs_78utNUznc",
    "Third_Grade_Communication_S2": "12edu3L3lWkiQTWqXkAWxzLaUo_4jXkXV",
    "Third_Grade_Communication_LastYear_S1": "1dnZ-B3w0eho4DLQuaUjcu_gYRxQVUh27",
    "Third_Grade_Communication_LastYear_S2": "1sHx6GHS5GGNfUWcAzJdDLXoxm5dJQUW5",
    "Third_Grade_Electronics_S1": "18s3_6cK3XaCGswmaaM3BqyqyWiE9adq0",
    "Third_Grade_Electronics_S2": "1g9QQto6aulAGb1s6jjBXStqnwyR3lWrI",
    "Third_Grade_Electronics_LastYear_S1": "14tpmEzBxeK0a-2xKLJh27-zCyCRY8Sm7",
    "Third_Grade_Electronics_LastYear_S2": "13RNcUE-TkB31oRZ_s8lGQ-7Y_UZDf_tM",
    "Third_Grade_Mechatronics_S1": "1pWHRg_DNnWHefQer6yfxf5SnNd4PFKVV",
    "Third_Grade_Mechatronics_S2": "16QAM_Dcbm9GPYOk5O5wi-Vo3WcS70Bus",
    "Third_Grade_Mechatronics_LastYear_S1": "1W49F5CEKwOeyHKgW5IQOUGttYtaqJoHr",
    "Third_Grade_Mechatronics_LastYear_S2": "1sNAFCyOQX31f04S80qFA5ZpZNQorPMSS",
    
    # Fourth Grade
    "Fourth_Grade_Communication_S1": "1I353V2Dd1END87jCYcZb33fOmhjjKnGJ",
    "Fourth_Grade_Communication_S2": "1oJaBS3_nLYjCXIYOgqM5enjYxymw5h59",
    "Fourth_Grade_Communication_LastYear_S1": "1eTqTDJc6_u3EsgzJKjjYU6metlTP7kZu",
    "Fourth_Grade_Communication_LastYear_S2": "",  # Empty - graduation project, no folder
    "Fourth_Grade_Electronics_S1": "1KOX51U4QKDJ3plORY7c__YVh7j4A26SH",
    "Fourth_Grade_Electronics_S2": "11I6Q4nEoiXC6lxpo3vTEsalbblIqRIqU",
    "Fourth_Grade_Electronics_LastYear_S1": "1Zuijl69NbI7qlLGJxrTjjaYLi61yeztV",
    "Fourth_Grade_Electronics_LastYear_S2": "1Auityu6tfsbBo_i3bAU8xFx7XQO5Tb-Z",
    "Fourth_Grade_Mechatronics_S1": "1x_uNebUvo3ZlqpciawuBnu0ooAg_pdfV",
    "Fourth_Grade_Mechatronics_S2": "1p5Y6tooBY9TVaz55mdYL7_xcJYzqY3nY",
    "Fourth_Grade_Mechatronics_LastYear_S1": "14GIx1XWA4bBtGZlcXJukX4PaB7iiaMZh",
    "Fourth_Grade_Mechatronics_LastYear_S2": "1oxDUpEZ7i9gYipvSeMWDx1E1pA6LFLaz",
}

# ---------------------------
# 4. Helpers
# ---------------------------
def fetch_recent_files_only(folder_id, minutes_back):
    """
    Fetch ONLY files modified in the last N minutes from a folder.
    This is the key to fast incremental syncs - we don't even fetch old files.
    """
    try:
        url = "https://www.googleapis.com/drive/v3/files"
        
        # Calculate cutoff time (e.g., 10 minutes ago)
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
        cutoff_str = cutoff_time.isoformat() + 'Z'
        
        # Query: files in this folder, not trashed, modified after cutoff
        query = f"'{folder_id}' in parents and trashed=false and modifiedTime > '{cutoff_str}'"
        
        params = {
            'q': query,
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
            
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params['pageToken'] = next_page_token
        
        return all_files
    
    except requests.RequestException as e:
        print(f"❌ Error fetching folder {folder_id}: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []

def fetch_all_files_recursive(folder_id, visited=None):
    """
    Fetch ALL files recursively (for initial sync).
    This is slower but thorough - used only on first run.
    """
    if visited is None:
        visited = set()
    
    if folder_id in visited:
        return []
    visited.add(folder_id)
    
    try:
        url = "https://www.googleapis.com/drive/v3/files"
        query = f"'{folder_id}' in parents and trashed=false"
        
        params = {
            'q': query,
            'key': API_KEY,
            'pageSize': 1000,
            'fields': 'nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime)',
        }
        
        all_files = []
        while True:
            res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            data = res.json()
            
            items = data.get("files", [])
            
            for item in items:
                if item.get("mimeType") == "application/vnd.google-apps.folder":
                    # Recurse into subfolder
                    subfolder_files = fetch_all_files_recursive(item['id'], visited)
                    all_files.extend(subfolder_files)
                else:
                    # It's a file
                    all_files.append(item)
            
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params['pageToken'] = next_page_token
        
        return all_files
    
    except Exception as e:
        print(f"❌ Error in recursive fetch: {e}")
        return []

def get_recent_files_recursive(folder_id, minutes_back, visited=None):
    """
    Recursively check subfolders but ONLY fetch recently modified files.
    Hybrid approach: traverse folder structure but filter by time.
    """
    if visited is None:
        visited = set()
    
    if folder_id in visited:
        return []
    visited.add(folder_id)
    
    all_files = []
    
    # Get items from this folder (will only return recent ones due to query filter)
    items = fetch_recent_files_only(folder_id, minutes_back)
    
    for item in items:
        if item.get("mimeType") == "application/vnd.google-apps.folder":
            # Even if folder itself is "recent", check inside it
            subfolder_files = get_recent_files_recursive(item['id'], minutes_back, visited)
            all_files.extend(subfolder_files)
        else:
            all_files.append(item)
    
    # Also check for subfolders that might not be recently modified themselves
    # but could contain recently modified files
    try:
        url = "https://www.googleapis.com/drive/v3/files"
        query = f"'{folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
        params = {
            'q': query,
            'key': API_KEY,
            'pageSize': 1000,
            'fields': 'files(id)',
        }
        
        res = requests.get(url, params=params, timeout=10)
        res.raise_for_status()
        folders = res.json().get("files", [])
        
        for folder in folders:
            if folder['id'] not in visited:
                subfolder_files = get_recent_files_recursive(folder['id'], minutes_back, visited)
                all_files.extend(subfolder_files)
    
    except Exception as e:
        print(f"⚠️  Warning checking subfolders: {e}")
    
    return all_files

def create_empty_topic_placeholder(topic):
    """
    Create placeholder entries in databases for empty folders.
    This ensures the topic exists in the database structure even with no files.
    """
    try:
        # Create placeholder in RTDB
        ref = rtdb.reference(f"updates/{topic}")
        if ref.get() is None:
            ref.set({"_placeholder": True, "timestamp": int(time.time())})
            print(f"   📝 Created RTDB placeholder for {topic}")
        
        # Create placeholder document in Firestore
        doc_ref = firestore_db.collection("updates").document(topic)
        if not doc_ref.get().exists:
            doc_ref.set({"_placeholder": True, "timestamp": int(time.time())})
            print(f"   📝 Created Firestore placeholder for {topic}")
        
        return True
    except Exception as e:
        print(f"   ⚠️  Warning: Could not create placeholder for {topic}: {e}")
        return False

def save_to_rtdb(topic, file):
    """Save file to Firebase Realtime Database."""
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
            return True
        return False
    
    except Exception as e:
        print(f"❌ RTDB error for {file.get('name', 'unknown')}: {e}")
        return False

def save_to_firestore(topic, file):
    """Save file to Firestore."""
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
            return True
        return False
    
    except Exception as e:
        print(f"❌ Firestore error for {file.get('name', 'unknown')}: {e}")
        return False

# ---------------------------
# 5. Main Logic
# ---------------------------
def process_folder_initial(topic, folder_id):
    """
    Initial sync: Fetch ALL files recursively (slow but thorough).
    Use this for the first run when database is empty.
    """
    print(f"📂 [INITIAL] {topic}")
    
    # Skip if folder ID is empty
    if not folder_id or folder_id.strip() == "":
        print(f"   ⚠️  No folder ID provided - skipping")
        return 0
    
    try:
        files = fetch_all_files_recursive(folder_id)
        print(f"   Found {len(files)} total files")
        
        # If no files found, create placeholder so topic exists in database
        if len(files) == 0:
            create_empty_topic_placeholder(topic)
            print(f"   ✓ Empty folder - placeholder created")
            return 0
        
        new_count = 0
        for f in files:
            new_in_rtdb = save_to_rtdb(topic, f)
            new_in_fs = save_to_firestore(topic, f)
            
            if new_in_rtdb or new_in_fs:
                new_count += 1
                if new_count <= 5:  # Only print first few to avoid spam
                    print(f"   ✅ {f['name']}")
        
        if new_count > 5:
            print(f"   ✅ ... and {new_count - 5} more files")
        
        print(f"   ✓ {topic}: {new_count} files added")
        return new_count
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return 0

def process_folder_incremental(topic, folder_id):
    """
    Incremental sync: Only check files modified in last N minutes (fast).
    Use this for regular cron runs - typically finds 0-5 new files.
    """
    print(f"⚡ [INCREMENTAL] {topic}")
    
    # Skip if folder ID is empty
    if not folder_id or folder_id.strip() == "":
        print(f"   ⚠️  No folder ID - skipping")
        return 0
    
    try:
        files = get_recent_files_recursive(folder_id, MINUTES_LOOKBACK)
        
        if not files:
            print(f"   No recent changes")
            return 0
        
        print(f"   Found {len(files)} recently modified files")
        
        new_count = 0
        for f in files:
            new_in_rtdb = save_to_rtdb(topic, f)
            new_in_fs = save_to_firestore(topic, f)
            
            if new_in_rtdb or new_in_fs:
                new_count += 1
                print(f"   ✅ NEW: {f['name']}")
        
        if new_count == 0:
            print(f"   ✓ All files already in database")
        else:
            print(f"   ✓ {new_count} new files added")
        
        return new_count
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return 0

def main():
    """Main execution."""
    start_time = time.time()
    
    print("="*70)
    if INITIAL_SYNC:
        print("🔄 INITIAL SYNC MODE - This will take a while...")
        print("   Fetching ALL files from ALL folders recursively")
        print("   This should only run ONCE to populate the database")
    else:
        print("⚡ INCREMENTAL SYNC MODE - Fast update")
        print(f"   Only checking files modified in last {MINUTES_LOOKBACK} minutes")
        print("   Perfect for regular cron runs every 5 minutes")
    print("="*70)
    print()
    
    total_new = 0
    total_folders = len(FOLDERS)
    
    for idx, (topic, folder_id) in enumerate(FOLDERS.items(), 1):
        print(f"\n[{idx}/{total_folders}] ", end="")
        
        if INITIAL_SYNC:
            new_count = process_folder_initial(topic, folder_id)
        else:
            new_count = process_folder_incremental(topic, folder_id)
        
        total_new += new_count
        
        # Small delay to be nice to APIs
        time.sleep(0.2)
    
    elapsed = time.time() - start_time
    
    print("\n" + "="*70)
    print(f"✅ COMPLETE")
    print(f"   Time: {elapsed:.1f} seconds")
    print(f"   New files added: {total_new}")
    print(f"   Mode: {'INITIAL SYNC' if INITIAL_SYNC else 'INCREMENTAL SYNC'}")
    print("="*70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        raise
