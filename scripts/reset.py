import os
from pymongo import MongoClient
from dotenv import load_dotenv


def main():
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DATABASE")
    
    if not uri or not db_name:
        print("Error: MONGO_URI or MONGO_DATABASE not set in environment.")
        return

    client = MongoClient(uri)
    db = client[db_name]
    
    collections = ["channel_news_asia", "mothership", "straits_times"]

    print("Starting field rename from 'needs_popularity_prediction' to 'needs_prediction'...")

    for col_name in collections:
        collection = db[col_name]
        print(f"Processing collection: {col_name}")
        
        result = collection.update_many(
            {"needs_popularity_prediction": {"$exists": True}},
            {"$rename": {"needs_popularity_prediction": "needs_prediction"}}
        )

        print(
            f"  - Matched: {result.matched_count}, Modified: {result.modified_count}"
        )
    
    print("Renaming complete.")
    client.close()


if __name__ == "__main__":
    main()
