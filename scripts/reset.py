import os
from pymongo import MongoClient
from dotenv import load_dotenv


def main():
    load_dotenv()
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DATABASE")]
    collection = db[os.getenv("MONGO_COLLECTION")]

    result = collection.update_many({}, {"$set": {"needs_update": True}})
    print(f"matched: {result.matched_count}, modified:{result.modified_count}")
    client.close()


if __name__ == "__main__":
    main()
