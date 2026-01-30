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

    collections = os.getenv("MONGO_COLLECTIONS", "").split(",")
    if "channel_news_asia" not in collections:
        collections.append("channel_news_asia")

    print("Starting field rename migration...")

    for col_name in collections:
        col_name = col_name.strip()
        if not col_name:
            continue

        collection = db[col_name]
        print(f"Processing collection: {col_name}")

        # Step 1: Rename 'semantic' to 'embedding'
        print("  - Renaming 'semantic' to 'embedding'...")
        result_1 = collection.update_many(
            {"semantic": {"$exists": True}}, {"$rename": {"semantic": "embedding"}}
        )
        print(
            f"    Matched: {result_1.matched_count}, Modified: {result_1.modified_count}"
        )

        # Step 2: Rename 'embedding.embedding' to 'embedding.vector'
        # Note: This runs on documents that have 'embedding' (either just renamed or existed)
        print("  - Renaming 'embedding.embedding' to 'embedding.vector'...")
        result_2 = collection.update_many(
            {"embedding.embedding": {"$exists": True}},
            {"$rename": {"embedding.embedding": "embedding.vector"}},
        )
        print(
            f"    Matched: {result_2.matched_count}, Modified: {result_2.modified_count}"
        )

    print("Migration complete.")
    client.close()


if __name__ == "__main__":
    main()
