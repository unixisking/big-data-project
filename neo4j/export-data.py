from neo4j import GraphDatabase

# --- CONFIGURATION ---
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
OUTPUT_FILE = "neo4j-results.txt"

def export_result_1():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        print(f"Exporting data to {OUTPUT_FILE}...")
        
        # Cypher query to fetch PMID, Title, and Abstract
        # We filter out nodes that don't have a title (the "ghost" nodes)
        query = """
        MATCH (a:Article)
        WHERE a.title IS NOT NULL AND a.title <> ""
        RETURN a.pmid AS pmid, a.title AS title, a.abstract AS abstract
        """
        
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            results = session.run(query)
            count = 0
            
            for record in results:
                pmid = record["pmid"]
                title = record["title"]
                abstract = record["abstract"]
                
                # Format: PMID/Title Abstract
                line = f"{pmid}/{title} {abstract}\n"
                f.write(line)
                
                count += 1
                if count % 5000 == 0:
                    print(f"Processed {count} lines...")

        print(f"SUCCESS: Exported {count} articles to {OUTPUT_FILE}")

    driver.close()

if __name__ == "__main__":
    export_result_1()