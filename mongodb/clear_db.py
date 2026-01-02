from pymongo import MongoClient

# Configuration (matching loose_importer.py)
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "litcovid_json"
COLLECTION_NAME = "bio_c_json"

def clear_database():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print(f"Connecting to {DB_NAME}.{COLLECTION_NAME}...")
    
    # Count documents before deletion
    count_before = collection.count_documents({})
    print(f"Found {count_before:,} documents in the collection.")
    
    if count_before == 0:
        print("Collection is already empty. Nothing to delete.")
        return
    
    # Confirm deletion
    response = input(f"Are you sure you want to delete all {count_before:,} documents? (yes/no): ")
    if response.lower() != 'yes':
        print("Deletion cancelled.")
        return
    
    # Delete all documents
    print("Deleting all documents...")
    result = collection.delete_many({})
    
    print(f"\nDeleted {result.deleted_count:,} documents successfully!")
    
    # Verify deletion
    count_after = collection.count_documents({})
    print(f"Remaining documents: {count_after:,}")

if __name__ == "__main__":
    clear_database()

