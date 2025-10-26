# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime, timezone
from typing import List, Optional

from itemadapter import ItemAdapter
from pymongo import MongoClient, UpdateOne


class MongoDBPipeline:
    """
    Cleans article fields and buffers MongoDB updates so writes are sent in batches.
    Uses the spider name as the target collection unless the spider defines
    `mongo_collection`.
    """

    SPECIAL_WHITESPACE = ("\xa0", "\u202f", "\u200b")
    TRANSLATION = str.maketrans({"\xa0": " ", "\u202f": " ", "\u200b": ""})

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler.settings.get("MONGO_URI"), crawler.settings.get("MONGO_DATABASE")
        )

    def __init__(self, uri: str, database: str, bulk_size: int = 50) -> None:
        self.uri = uri
        self.database = database
        self.bulk_size = max(1, bulk_size)

        self.client: Optional[MongoClient] = None
        self.collection = None
        self._operations: List[UpdateOne] = []
        self.logger = None

    def open_spider(self, spider) -> None:
        if not self.uri or not self.database:
            raise RuntimeError("Mongo connection settings are required.")

        collection_name = getattr(spider, "mongo_collection", spider.name)
        self.client = MongoClient(self.uri)
        self.collection = self.client[self.database][collection_name]
        self._operations = []
        self.logger = spider.logger

    def close_spider(self, spider) -> None:
        self._flush()

        if self.client:
            self.client.close()
            self.client = None
            self.collection = None
            self.logger = None

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        document_id = adapter.get("_id")

        article = {
            field: self._clean_value(value)
            for field, value in adapter.items()
            if field != "_id"
        }

        update = UpdateOne(
            {"_id": document_id},
            {
                "$set": {
                    "article": article,
                    "needs_scraping": False,
                }
            },
        )
        self._operations.append(update)

        if len(self._operations) >= self.bulk_size:
            self._flush()

        return item

    def _flush(self) -> None:
        if self.collection is None or not self._operations:
            return

        try:
            result = self.collection.bulk_write(self._operations, ordered=False)
            inserted = result.upserted_count
            modified = result.modified_count
            matched = result.matched_count
            if self.logger:
                self.logger.info(
                    "MongoDBPipeline bulk_write: matched=%s modified=%s upserted=%s",
                    matched,
                    modified,
                    inserted,
                )
        finally:
            self._operations = []

    def _clean_value(self, value):
        if isinstance(value, str):
            if any(char in value for char in self.SPECIAL_WHITESPACE):
                value = value.translate(self.TRANSLATION)
            return value.strip()

        if isinstance(value, list):
            return [self._clean_value(item) for item in value]

        if isinstance(value, dict):
            return {key: self._clean_value(val) for key, val in value.items()}

        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

        return value
