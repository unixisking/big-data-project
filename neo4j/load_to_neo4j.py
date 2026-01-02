import ijson
import json
from neo4j import GraphDatabase

# --- CONFIGURATION ---
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
FILE_PATH = '../data/neo4j/litcovid2BioCJSON-converted'

def stream_json(path):
    with open(path, 'rb') as f:
        for obj in ijson.items(f, 'item'):
            yield obj

def import_to_neo4j():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        print("Starting Neo4j Import (with Null Guards)...")
        
        count = 0
        skipped = 0
        for doc in stream_json(FILE_PATH):
            pmid = None
            title = ""
            abstract_parts = []
            refs = []

            # --- EXTRACTION WITH SAFETY ---
            for p in doc.get('passages', []):
                infons = p.get('infons', {})
                
                # Check for PMID in this passage
                if not pmid:
                    pmid = infons.get('article-id_pmid')
                
                if infons.get('section_type') == 'TITLE':
                    title = p.get('text', '')
                
                if infons.get('section_type') == 'ABSTRACT' and infons.get('type') == 'abstract':
                    abstract_parts.append(p.get('text', ''))
                
                if infons.get('section_type') == 'REF':
                    ref_id = infons.get('pub-id_pmid')
                    if ref_id: 
                        refs.append(ref_id)

            # --- THE NULL GUARD ---
            if not pmid:
                skipped += 1
                # print(f"Skipping document with no PMID")
                continue # Skip this object (likely the legal notice or bad data)

            full_abstract = " ".join(abstract_parts)

            cypher = """
            MERGE (a:Article {pmid: $pmid})
            SET a.title = $title, a.abstract = $abstract
            WITH a
            UNWIND $refs AS ref_pmid
            // Pass variables to a WITH clause to enable the WHERE filter
            WITH a, ref_pmid 
            WHERE ref_pmid IS NOT NULL AND ref_pmid <> ""
            MERGE (target:Article {pmid: ref_pmid})
            MERGE (a)-[:CITES]->(target)
            """
            try:
                session.run(cypher, pmid=pmid, title=title, abstract=full_abstract, refs=refs)
                count += 1
                if count % 100 == 0:
                    print(f"Imported: {count} | Skipped: {skipped}...")
            except Exception as e:
                print(f"Unexpected Error at PMID {pmid}: {e}")

    driver.close()
    print(f"DONE! Final Count: {count} | Total Skipped: {skipped}")

if __name__ == "__main__":
    import_to_neo4j()