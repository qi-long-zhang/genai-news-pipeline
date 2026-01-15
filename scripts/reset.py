import os
from pymongo import MongoClient
from dotenv import load_dotenv


def main():
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    client = MongoClient(uri)
    db = client[os.getenv("MONGO_DATABASE")]
    collection = db[os.getenv("MONGO_COLLECTION")]

    print("开始更新...")

    # 逻辑：
    # 1. 筛选：只找有 source_account 的文档
    # 2. 修改：设置 source 为 "Mothership"
    # 3. 删除：删除 source_account 字段
    result = collection.update_many(
        {"source_account": {"$exists": True}},
        {"$set": {"source": "Mothership"}, "$unset": {"source_account": ""}},
    )

    print(f"匹配数量: {result.matched_count}, 修改数量: {result.modified_count}")
    client.close()


if __name__ == "__main__":
    main()
