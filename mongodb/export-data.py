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

    print(f"Exporting Result 1 to {OUTPUT_FILE}...")

    # We only project the 'passages' field to save bandwidth and RAM
    cursor = col.find({}, {"passages": 1})

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        count = 0
        for doc in cursor:
            pmid = None
            title = ""
            abstract_parts = []
            
            passages = doc.get("passages", [])
            for p in passages:
                infons = p.get("infons", {})
                
                # Extract PMID
                if not pmid:
                    pmid = infons.get("article-id_pmid")
                
                # Extract Title
                if infons.get("section_type") == "TITLE" or infons.get("type") == "title":
                    title = p.get("text", "")
                
                # Extract Abstract
                if infons.get("section_type") == "ABSTRACT" or infons.get("type") == "abstract":
                    abstract_parts.append(p.get("text", ""))

            if pmid:
                full_abstract = " ".join(abstract_parts)
                # Format: PMID/Title Abstract
                f.write(f"{pmid}/{title} {full_abstract}\n")
                
            count += 1
            if count % 10000 == 0:
                print(f"Processed {count} documents...")

    print(f"SUCCESS: Exported {count} lines to {OUTPUT_FILE}")
    client.close()

if __name__ == "__main__":
    export_mongodb_data()