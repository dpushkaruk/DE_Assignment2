import mysql.connector
import json
import os
import random

FOLDER_PATH = r"C:\Users\dpush\Documents\DataEngineering_Ass2\include\telephony_logs"

SUMMARIES = [
    "LLM Summary: Caller requested a password reset. Agent authenticated and sent reset link.",
    "LLM Summary: Customer complained about unexpected billing charge. Agent explained prorated fees.",
    "LLM Summary: Technical issue with the software installation. Agent escalated to Tier 2 support.",
    "LLM Summary: Dropped call. Agent attempted to call back but reached voicemail.",
    "LLM Summary: User wanted to cancel service. Agent offered retention discount, user accepted."
]

print("Checking existing JSON files...")
existing_files = [f for f in os.listdir(FOLDER_PATH) if f.endswith('.json')]
existing_ids = set([int(f.replace('.json', '')) for f in existing_files])

try:
    db = mysql.connector.connect(
        host="localhost", 
        user="root",          
        password="secretka",  
        database="support_call_center"
    )
    cursor = db.cursor()

    cursor.execute("SELECT call_id FROM calls")
    all_db_ids = [row[0] for row in cursor.fetchall()]

    new_ids = [cid for cid in all_db_ids if cid not in existing_ids]

    if not new_ids:
        print("no new calls found in MySQL")
    else:
        print(f"found {len(new_ids)} new calls, generating JSON mock API data")

        generated_count = 0
        for i, call_id in enumerate(new_ids):
            data = {
                "call_id": call_id,
                "duration_sec": random.randint(15, 1200),
                "short_description": random.choice(SUMMARIES)
            }
            
            if i == len(new_ids):
                del data["duration_sec"]
                data["short_description"] = "ERROR: Missing duration_sec"

            file_path = os.path.join(FOLDER_PATH, f"{call_id}.json")
            with open(file_path, "w") as f:
                json.dump(data, f, indent=4)
                
            generated_count += 1

        print(f"\nGenerated {generated_count} new JSON files in {FOLDER_PATH}")

except mysql.connector.Error as err:
    print(f"error connecting to MySQL: {err}")
finally:
    if 'db' in locals() and db.is_connected():
        cursor.close()
        db.close()