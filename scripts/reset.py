import os
from pymongo import MongoClient
from dotenv import load_dotenv


def main():
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    client = MongoClient(uri)
    db = client[os.getenv("MONGO_DATABASE")]
    collection = db["straits_times"]

    print("开始重置 needs_scraping 字段...")

    result = collection.update_many(
        {},  # filter: all documents
        {"$set": {"needs_scraping": True}},
    )

    print(
        f"重置完成。匹配文档: {result.matched_count}, 修改文档: {result.modified_count}"
    )
    client.close()


if __name__ == "__main__":
    main()
