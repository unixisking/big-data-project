from pymongo import MongoClient

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "litcovid_json"
COLLECTION_NAME = "bio_c_json"
OUTPUT_FILE = "./mongo-refs.txt"

def export_refs():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    print(f"Generating {OUTPUT_FILE} from MongoDB...")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        cursor = col.find({}, {"passages": 1})
        count = 0

        for doc in cursor:
            source_pmid = None
            references = []

            for passage in doc.get("passages", []):
                infons = passage.get("infons", {})
                
                # Identify the Source PMID
                if not source_pmid:
                    source_pmid = infons.get("article-id_pmid")
                
                # Identify References
                if infons.get("section_type") == "REF":
                    ref_id = infons.get("pub-id_pmid")
                    if ref_id:
                        references.append(ref_id)

            # Only write to file if we have a source and at least one reference
            if source_pmid and references:
                ref_string = "/".join(references)
                f.write(f"{source_pmid}/{ref_string}\n")
                
                count += 1
                if count % 10000 == 0:
                    print(f"Exported {count} citation lines...")

    print(f"SUCCESS: Exported {count} source-to-reference lines.")
    client.close()

if __name__ == "__main__":
    export_refs()