import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime


def remove_duplicates_in_collection(db, collection_name):
    print(f"--- Processing collection: {collection_name} ---")
    collection = db[collection_name]

    # Aggregation pipeline to find duplicates based on article_url
    pipeline = [
        {"$match": {"article_url": {"$exists": True, "$ne": None}}},
        {
            "$group": {
                "_id": "$article_url",
                "count": {"$sum": 1},
                "docs": {"$push": {"_id": "$_id", "created_at": "$created_at"}},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]

    duplicates = list(collection.aggregate(pipeline))
    print(f"Found {len(duplicates)} groups of duplicate article_urls")

    deleted_count = 0
    for group in duplicates:
        docs = group["docs"]
        
        # Sort by created_at descending (newest first)
        # Use datetime.min as default if created_at is missing/None
        docs.sort(
            key=lambda x: x.get("created_at") or datetime.min, 
            reverse=True
        )

        # Keep the first one (newest), delete the rest
        to_delete = [doc["_id"] for doc in docs[1:]]
        
        if to_delete:
            result = collection.delete_many({"_id": {"$in": to_delete}})
            deleted_count += result.deleted_count

    print(f"Cleanup complete. Deleted {deleted_count} duplicate documents.")


def main():
    load_dotenv()
    
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db_name = os.getenv("MONGO_DATABASE")     
    mongo_collections = os.getenv("MONGO_COLLECTIONS").split(",")
    
    if not mongo_collections:
        print("No collections to process.")
        return

    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]

    for collection_name in mongo_collections:
        remove_duplicates_in_collection(db, collection_name)

    client.close()


if __name__ == "__main__":
    main()
