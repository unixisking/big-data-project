from BaseXClient import BaseXClient

# --- CONFIGURATION ---
SESSION = BaseXClient.Session('localhost', 1984, 'admin', 'admin')
OUTPUT_FILE = "basex_result2.txt"

def export_basex_refs():
    print("Streaming citations from BaseX using the corrected XML paths...")
    
    # We use your logic: 'infon' (singular), 'article-id_pmid', and 'REF' section_type.
    # We iterate over the single database 'litcovid_xml'.
    query_string = """
    for $article in db:open('litcovid_xml')//document
    let $source_id := $article//infon[@key = 'article-id_pmid']/text()
    let $ref_ids := $article//passage[infon[@key='section_type'] = 'REF']//infon[@key='pub-id_pmid']/text()
    where exists($source_id) and exists($ref_ids)
    return concat($source_id[1], "/", string-join($ref_ids, "/"))
    """
    
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            query = SESSION.query(query_string)
            count = 0
            
            # Using .iter() to stream (typecode, item) pairs from the server
            for typecode, item in query.iter():
                if item:
                    f.write(item + "\n")
                    count += 1
                
                if count % 5000 == 0 and count > 0:
                    print(f"Exported {count} citation rows...")
            
            query.close()
            
        print(f"SUCCESS: Exported {count} lines to {OUTPUT_FILE}.")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        SESSION.close()

if __name__ == "__main__":
    export_basex_refs()