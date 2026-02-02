import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")


def remove_duplicates():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]

    for col_name in MONGO_COLLECTIONS:
        collection = db[col_name]
        print(f"Processing collection: {col_name}")

        # Aggregation pipeline to find duplicates by article title
        # We only look at documents where article.title exists
        pipeline = [
            {"$match": {"article.title": {"$exists": True, "$ne": ""}}},
            {
                "$group": {
                    "_id": "$article.title",
                    "ids": {"$push": {"id": "$_id", "created_at": "$created_at"}},
                    "count": {"$sum": 1},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]

        cursor = collection.aggregate(pipeline)

        total_deleted = 0
        for doc in cursor:
            # Sort duplicates by created_at descending (latest first)
            sorted_docs = sorted(
                doc["ids"],
                key=lambda x: x.get("created_at"),
                reverse=True,
            )

            # Keep the first one (latest), delete the rest
            ids_to_delete = [d["id"] for d in sorted_docs[1:]]

            if ids_to_delete:
                result = collection.delete_many({"_id": {"$in": ids_to_delete}})
                total_deleted += result.deleted_count

        print(f"Deleted {total_deleted} duplicate documents in '{col_name}'.")

    client.close()


if __name__ == "__main__":
    remove_duplicates()
