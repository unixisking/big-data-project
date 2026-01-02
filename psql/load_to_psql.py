import json
import psycopg2
from psycopg2.extras import execute_values
import ijson  # Install with: pip install ijson

# --- CONFIGURATION ---
DB_PARAMS = {
    "dbname": "litcovid_db",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

FILE_PATH = '../data/postgres/litcovid2BioCJSON-converted'

def stream_json_objects(path):
    """
    Streams individual objects from a large JSON array.
    This prevents memory crashes with your 9GB file.
    """
    with open(path, 'rb') as f:
        # ijson.items(f, 'item') handles the [ {}, {}, ... ] structure
        for obj in ijson.items(f, 'item'):
            try:
                # Return as a tuple (json_string,) for PostgreSQL JSONB
                yield (json.dumps(obj),)
            except (json.JSONDecodeError, TypeError):
                continue

def run_import():
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        print(f"Starting import of {FILE_PATH}...")
        query = "INSERT INTO bioc_data (document) VALUES %s"
        
        batch = []
        total_count = 0
        
        for doc_tuple in stream_json_objects(FILE_PATH):
            batch.append(doc_tuple)
            
            if len(batch) >= 500:
                execute_values(cur, query, batch)
                conn.commit()
                total_count += len(batch)
                batch = []
                print(f"Imported {total_count} documents...")

        if batch:
            execute_values(cur, query, batch)
            conn.commit()
            total_count += len(batch)

        print(f"SUCCESS: Imported {total_count} documents total.")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    run_import()