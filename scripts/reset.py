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

    # Target collections to reset
    collections = ["straits_times"]

    print("Starting reset of 'needs_scraping' field...")

    for col_name in collections:
        collection = db[col_name]
        print(f"Processing collection: {col_name}")

        result = collection.update_many({}, {"$set": {"needs_scraping": True}})
        print(
            f"    Matched: {result.matched_count}, Modified: {result.modified_count}"
        )

    print("Reset complete.")
    client.close()


if __name__ == "__main__":
    main()