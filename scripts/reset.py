import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime


def main():
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    client = MongoClient(uri)
    db = client[os.getenv("MONGO_DATABASE")]
    collection = db["straits_times"]

    print("开始清理重复数据...")

    # Aggregation pipeline to find duplicates
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
    print(f"发现 {len(duplicates)} 组重复的 article_url")

    deleted_count = 0
    for group in duplicates:
        docs = group["docs"]
        # Sort by created_at descending (newest first)
        # Note: created_at might be None or missing, so we need to handle that safely.
        # Assuming created_at is a datetime object or comparable.
        docs.sort(
            key=lambda x: x.get("created_at") or datetime.min, reverse=True
        )

        # Keep the first one (newest), delete the rest
        to_delete = [doc["_id"] for doc in docs[1:]]
        
        if to_delete:
            result = collection.delete_many({"_id": {"$in": to_delete}})
            deleted_count += result.deleted_count

    print(f"清理完成。共删除了 {deleted_count} 个重复文档。")
    client.close()


if __name__ == "__main__":
    main()
