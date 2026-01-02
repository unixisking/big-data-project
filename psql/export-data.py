import psycopg2

# --- CONFIGURATION ---
DB_PARAMS = {
    "dbname": "litcovid_db",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}
OUTPUT_FILE = "./postgres-data.txt"

def export_postgres_data():
    conn = psycopg2.connect(**DB_PARAMS)
    # We give the cursor a name to enable "Server-Side" streaming
    cur = conn.cursor(name="fetch_result1")
    
    print(f"Exporting Result 1 to {OUTPUT_FILE}...")
    
    # Query logic:
    # 1. Expand the 'passages' array
    # 2. Extract PMID from infons
    # 3. Filter for Title and Abstract sections
    query = """
    SELECT 
        document->'passages'->0->'infons'->>'article-id_pmid' as pmid,
        (SELECT p->>'text' FROM jsonb_array_elements(document->'passages') p 
         WHERE p->'infons'->>'section_type' = 'TITLE' LIMIT 1) as title,
        (SELECT string_agg(p->>'text', ' ') FROM jsonb_array_elements(document->'passages') p 
         WHERE p->'infons'->>'section_type' = 'ABSTRACT') as abstract
    FROM bioc_data;
    """
    
    cur.execute(query)
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        count = 0
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
                
            for pmid, title, abstract in rows:
                if pmid:
                    # Format: PMID/Title Abstract
                    f.write(f"{pmid}/{title or ''} {abstract or ''}\n")
                    count += 1
            
            if count % 10000 == 0:
                print(f"Processed {count} documents...")

    cur.close()
    conn.close()
    print(f"SUCCESS: Exported {count} lines.")

if __name__ == "__main__":
    export_postgres_data()