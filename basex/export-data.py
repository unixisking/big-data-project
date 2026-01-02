from BaseXClient import BaseXClient

# --- CONFIGURATION ---
SESSION = BaseXClient.Session('localhost', 1984, 'admin', 'admin')
OUTPUT_FILE = "basex_result1.txt"

def export_basex_data():
    print("Streaming Title and Abstract data from BaseX...")
    
    # FIXED XQUERY: Using [1] to handle sequences and string-join for safety
    query_string = """
    for $article in db:open('litcovid_xml')//document
    let $pmid := $article//infon[@key='article-id_pmid']/text()
    let $title := $article//passage[infon[@key='section_type']='TITLE']/text/text()
    let $abstract_parts := $article//passage[infon[@key='section_type']='ABSTRACT']/text/text()
    where exists($pmid)
    (: We use [1] on $pmid and $title to ensure we only have one item for concat :)
    return concat($pmid[1], "/", string-join($title, " "), " ", string-join($abstract_parts, " "))
    """
    
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            query = SESSION.query(query_string)
            count = 0
            
            # Using the .iter() protocol to stream (typecode, item)
            for typecode, item in query.iter():
                if item:
                    # Clean up any newlines within the text to keep one article per line
                    cleaned_item = " ".join(item.splitlines())
                    f.write(cleaned_item + "\n")
                    count += 1
                
                if count % 5000 == 0 and count > 0:
                    print(f"Exported {count} articles...")
            
            query.close()
            
        print(f"SUCCESS: Exported {count} lines to {OUTPUT_FILE}.")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        SESSION.close()

if __name__ == "__main__":
    export_basex_data()