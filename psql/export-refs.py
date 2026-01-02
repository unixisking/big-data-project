import psycopg2

# --- CONFIGURATION ---
DB_PARAMS = {
    "dbname": "litcovid_db",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}
OUTPUT_FILE = "./postgres-refs.txt"

def export_postgres_refs():
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        # Using a named cursor creates a server-side cursor for streaming
        cur = conn.cursor(name="citation_stream_cursor")
        
        print(f"Streaming data from PostgreSQL to {OUTPUT_FILE}...")

        # SQL Logic:
        # 1. Get the source PMID from the first passage's metadata.
        # 2. Subquery: Expand 'passages', filter for 'REF' sections, 
        #    and aggregate the reference PMIDs with a '/'.
        query = """
        SELECT 
            document->'passages'->0->'infons'->>'article-id_pmid' as source_pmid,
            (
                SELECT string_agg(p->'infons'->>'pub-id_pmid', '/')
                FROM jsonb_array_elements(document->'passages') AS p
                WHERE p->'infons'->>'section_type' = 'REF'
            ) as cited_pmids
        FROM bioc_data
        WHERE document->'passages'->0->'infons'->>'article-id_pmid' IS NOT NULL;
        """

        cur.execute(query)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            count = 0
            while True:
                # Fetching in batches to keep memory usage low
                rows = cur.fetchmany(2000)
                if not rows:
                    break
                
                for source_pmid, cited_pmids in rows:
                    # Only write if the article actually has references
                    if cited_pmids:
                        f.write(f"{source_pmid}/{cited_pmids}\n")
                        count += 1
                
                if count % 10000 == 0 and count > 0:
                    print(f"Exported {count} citation rows...")

        print(f"SUCCESS: Exported {count} citation rows total.")

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    export_postgres_refs()