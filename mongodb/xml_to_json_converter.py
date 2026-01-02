#!/usr/bin/env python3
"""
Convert BioC XML format to JSON format.
Handles large files using streaming XML parsing.
"""

import json
import xml.etree.ElementTree as ET
import sys
from pathlib import Path


def parse_infon(infon_elem):
    """Parse an infon element and return key-value pair."""
    key = infon_elem.get('key', '')
    value = infon_elem.text if infon_elem.text else ''
    return key, value


def parse_passage(passage_elem):
    """Parse a passage element and return a dictionary."""
    passage = {}
    infons = {}
    offset = None
    text = None
    
    for child in passage_elem:
        if child.tag == 'infon':
            key, value = parse_infon(child)
            if key:
                infons[key] = value
        elif child.tag == 'offset':
            offset = child.text if child.text else None
        elif child.tag == 'text':
            text = child.text if child.text else None
    
    if infons:
        passage['infons'] = infons
    if offset is not None:
        # Try to convert offset to int, otherwise keep as string
        try:
            passage['offset'] = int(offset)
        except (ValueError, AttributeError):
            passage['offset'] = offset
    if text is not None:
        passage['text'] = text
    
    return passage


def parse_document(doc_elem):
    """Parse a document element and return a dictionary."""
    doc = {}
    doc_id = None
    passages = []
    
    for child in doc_elem:
        if child.tag == 'id':
            doc_id = child.text if child.text else None
        elif child.tag == 'passage':
            passage = parse_passage(child)
            if passage:
                passages.append(passage)
    
    if doc_id:
        doc['id'] = doc_id
    if passages:
        doc['passages'] = passages
    
    return doc


def convert_xml_to_json(input_file, output_file):
    """
    Convert BioC XML file to JSON format.
    Uses iterative parsing to handle large files efficiently.
    """
    print(f"Reading XML file: {input_file}")
    print(f"Output JSON file: {output_file}")
    
    doc_count = 0
    
    try:
        # Use iterparse for memory-efficient parsing
        # We need to track the root element for cleanup
        context = ET.iterparse(input_file, events=('start', 'end'))
        context = iter(context)
        event, root = next(context)
        
        # Open output file for writing
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_doc = True
            
            for event, elem in context:
                if event == 'end' and elem.tag == 'document':
                    # Parse the complete document
                    doc = parse_document(elem)
                    if doc:
                        # Write document to file incrementally
                        if not first_doc:
                            f.write(',\n')
                        else:
                            first_doc = False
                        
                        # Write with proper indentation for array elements
                        doc_json = json.dumps(doc, ensure_ascii=False, indent=2)
                        # Indent each line of the document
                        indented = '\n'.join('  ' + line if line.strip() else line 
                                            for line in doc_json.split('\n'))
                        f.write(indented)
                        doc_count += 1
                        if doc_count % 100 == 0:
                            print(f"Processed {doc_count} documents...")
                    
                    # Clear the element and its children to free memory
                    elem.clear()
                    # Clear root's children to free memory (keep only the last few)
                    # This helps with memory management for large files
                    if len(root) > 10:
                        # Remove old children, keep recent ones
                        for child in list(root)[:-5]:
                            root.remove(child)
            
            f.write('\n]')
        
        print(f"\nTotal documents parsed: {doc_count}")
        print(f"Conversion complete! {doc_count} documents written to {output_file}")
        
    except ET.ParseError as e:
        print(f"XML parsing error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    input_file = Path('../data/basex/litcovid2BioCXML')
    output_file = Path('../data/basex/litcovid2BioCJSON')
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    
    convert_xml_to_json(input_file, output_file)


if __name__ == "__main__":
    main()

