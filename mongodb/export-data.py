import time
from pymongo import MongoClient

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "litcovid_json"
COLLECTION_NAME = "bio_c_json" 
OUTPUT_FILE = "mongo_result1.txt"

def export_mongodb_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    print(f"🚀 Starting optimized data export (PMID/Title/Abstract) to {OUTPUT_FILE}...")
    start_time = time.perf_counter()

    # The Pipeline logic:
    # 1. Match docs with PMID (using your index)
    # 2. Unwind passages to treat each block individually
    # 3. Group by the original document ID to rebuild the specific strings we need
    pipeline = [
        {"$match": {"passages.infons.article-id_pmid": {"$exists": True}}},
        {"$unwind": "$passages"},
        {"$group": {
            "_id": "$_id",
            # Get the source PMID (the first non-null found in any passage)
            "pmid": {"$max": "$passages.infons.article-id_pmid"},
            # Grab the title if the section_type is TITLE
            "title": {
                "$max": {
                    "$cond": [
                        {"$eq": ["$passages.infons.section_type", "TITLE"]}, 
                        "$passages.text", 
                        ""
                    ]
                }
            },
            # Push all abstract text blocks into an array
            "abstract_parts": {
                "$push": {
                    "$cond": [
                        {"$eq": ["$passages.infons.section_type", "ABSTRACT"]}, 
                        "$passages.text", 
                        None
                    ]
                }
            }
        }},
        # Filter out documents that somehow ended up without a PMID
        {"$match": {"pmid": {"$ne": None}}}
    ]

    try:
        cursor = col.aggregate(pipeline, allowDiskUse=True)
        count = 0
        
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for doc in cursor:
                pmid = doc.get('pmid')
                title = doc.get('title', "").strip()
                
                # Filter out None values and join the abstract parts
                abstracts = [a for a in doc.get('abstract_parts', []) if a]
                full_abstract = " ".join(abstracts).strip()
                
                # Clean up internal newlines to keep the "one line per article" format
                line = f"{pmid}/{title} {full_abstract}"
                cleaned_line = " ".join(line.splitlines())
                
                f.write(cleaned_line + "\n")
                count += 1
                
                if count % 10000 == 0 and count > 0:
                    print(f"📦 Exported {count} data rows...")

        duration = time.perf_counter() - start_time
        print(f"✅ SUCCESS: Exported {count} lines in {duration:.2f} seconds.")

    except Exception as e:
        print(f"❌ ERROR: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    export_mongodb_data()