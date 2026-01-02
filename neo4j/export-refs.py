from neo4j import GraphDatabase

# --- CONFIGURATION ---
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
OUTPUT_FILE = "Result2.txt"

def export_refs():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        print(f"Generating {OUTPUT_FILE} from Neo4j...")
        
        # We use collect(r.pmid) to group all citations for one article into a list
        query = """
        MATCH (a:Article)-[:CITES]->(r:Article)
        WHERE a.title IS NOT NULL AND a.title <> ""
        RETURN a.pmid AS source_pmid, collect(r.pmid) AS cited_list
        """
        
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            # We use a result buffer (streaming)
            results = session.run(query)
            count = 0
            
            for record in results:
                source = record["source_pmid"]
                cited_list = record["cited_list"]
                
                # Format: PMID/Ref1/Ref2/Ref3...
                # We join the list in Python - it's faster and doesn't require APOC
                citations = "/".join(str(c) for c in cited_list)
                line = f"{source}/{citations}\n"
                
                f.write(line)
                
                count += 1
                if count % 5000 == 0:
                    print(f"Exported {count} citation rows...")

        print(f"SUCCESS: Created {OUTPUT_FILE} with {count} source articles.")

    driver.close()

if __name__ == "__main__":
    export_refs()