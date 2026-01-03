import time
from pymongo import MongoClient

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "litcovid_json"
# ATTENTION : Vérifiez si votre collection s'appelle 'litcovid' ou 'bio_c_json'
COLLECTION_NAME = "bio_c_json" 
OUTPUT_FILE = "./mongo-refs.txt"

def export_mongo_refs():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    start_time = time.perf_counter()

    # Pipeline complète pour extraire les PMIDs sources et les références
    pipeline = [
        # 1. On filtre les docs qui ont un PMID (Utilise votre INDEX)
        {"$match": {"passages.infons.article-id_pmid": {"$exists": True}}},
        
        # 2. On transforme chaque passage en un document séparé
        {"$unwind": "$passages"},
        
        # 3. On projette les infos utiles pour simplifier le groupement
        {"$project": {
            "source_pmid": "$passages.infons.article-id_pmid",
            "is_ref": {"$eq": ["$passages.infons.section_type", "REF"]},
            "ref_pmid": "$passages.infons.pub-id_pmid"
        }},
        
        # 4. On groupe par l'ID technique de l'article (_id)
        {"$group": {
            "_id": "$_id",
            # On récupère le PMID source (le premier non nul trouvé)
            "source": {"$max": "$source_pmid"},
            # On accumule les PMIDs de références uniquement si section_type == 'REF'
            "refs": {
                "$push": {
                    "$cond": [{"$eq": ["$is_ref", True]}, "$ref_pmid", None]
                }
            }
        }},
        
        # 5. On ne garde que les articles qui ont au moins une référence
        {"$match": {"source": {"$ne": None}}}
    ]

    try:
        cursor = col.aggregate(pipeline, allowDiskUse=True)
        count = 0
        
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for doc in cursor:
                source = doc.get('source')
                # Nettoyage de la liste des références (enlève les None et doublons)
                raw_refs = doc.get('refs', [])
                clean_refs = sorted(list(set([str(r) for r in raw_refs if r])))

                if source and clean_refs:
                    f.write(f"{source}/{'/'.join(clean_refs)}\n")
                    count += 1
                
                if count % 5000 == 0 and count > 0:
                    print(f"Lignes exportées : {count}...")

        duration = time.perf_counter() - start_time
        print(f"SUCCÈS : {count} lignes exportées en {duration:.2f} secondes.")

    except Exception as e:
        print(f"ERREUR : {e}")
    finally:
        client.close()

if __name__ == "__main__":
    export_mongo_refs()