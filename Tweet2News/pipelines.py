# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import re
from datetime import datetime, timedelta, timezone

from itemadapter import ItemAdapter
from pymongo import MongoClient, UpdateOne


class MongoPipeline:
    """
    Cleans article fields and buffers MongoDB updates so writes are sent in batches.
    Uses the spider name as the target collection unless the spider defines
    `mongo_collection`.
    """

    TRANSLATION = str.maketrans(
        {
            "\xa0": " ",  # non-breaking space
            "\u202f": " ",  # narrow no-break space
            "\u200b": "",  # zero-width space
            "\u200c": "",  # zero-width non-joiner
            "\u200d": "",  # zero-width joiner
            "\u00ad": "",  # soft hyphen
        }
    )
    MULTI_SPACE = re.compile(r" {2,}")

    def __init__(self, crawler, mongo_uri, mongo_db, bulk_size=50):
        self.crawler = crawler
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.bulk_size = max(1, bulk_size)
        self.client = None
        self.collection = None
        self.logger = None
        self._operations = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE"),
        )

    def open_spider(self):
        if not self.mongo_uri or not self.mongo_db:
            raise RuntimeError("Mongo connection settings are required.")

        spider = self.crawler.spider
        collection_name = getattr(spider, "mongo_collection", spider.name)
        self.client = MongoClient(self.mongo_uri)
        self.collection = self.client[self.mongo_db][collection_name]
        self._operations = []
        self.logger = spider.logger

    def close_spider(self):
        self._flush()

        if self.client:
            self.client.close()
            self.client = None
            self.collection = None
            self.logger = None

    def process_item(self, item):
        adapter = ItemAdapter(item)
        document_id = adapter.get("_id")

        article = {
            field: self._clean_value(value)
            for field, value in adapter.items()
            if field != "_id"
        }

        # Determine if popularity prediction is needed based on publish_date
        publish_date = article.get("publish_date")
        needs_prediction = False
        if isinstance(publish_date, datetime):
            needs_prediction = datetime.now(timezone.utc) - publish_date <= timedelta(
                hours=24
            )

        update = UpdateOne(
            {"_id": document_id},
            {
                "$set": {
                    "article": article,
                    "needs_scraping": False,
                    "needs_prediction": needs_prediction,
                }
            },
            upsert=True,
        )
        self._operations.append(update)

        if len(self._operations) >= self.bulk_size:
            self._flush()

        return item

    def _flush(self):
        if self.collection is None or self.logger is None or not self._operations:
            return

        try:
            result = self.collection.bulk_write(self._operations, ordered=False)
            inserted = result.upserted_count
            modified = result.modified_count
            matched = result.matched_count
            self.logger.warning(
                "MongoPipeline bulk_write: matched=%s modified=%s upserted=%s",
                matched,
                modified,
                inserted,
            )
        finally:
            self._operations = []

    def _clean_value(self, value):
        if isinstance(value, str):
            value = value.translate(self.TRANSLATION)
            value = self.MULTI_SPACE.sub(" ", value)
            return value.strip()

        if isinstance(value, list):
            return [self._clean_value(item) for item in value]

        if isinstance(value, dict):
            return {key: self._clean_value(val) for key, val in value.items()}

        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

        return value
