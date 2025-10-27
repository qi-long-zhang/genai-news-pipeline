# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsArticleItem(scrapy.Item):
    """Normalized representation of a scraped news article."""

    # Primary reference back to the tweet document
    _id = scrapy.Field()
    article_url = scrapy.Field()

    # Article metadata
    title = scrapy.Field()
    subtitle = scrapy.Field()
    summary = scrapy.Field()
    author = scrapy.Field()
    publish_date = scrapy.Field()
    update_date = scrapy.Field()

    # Article body
    content = scrapy.Field()

    # Structured data
    tags = scrapy.Field()
    images = scrapy.Field()
    videos = scrapy.Field()
    links = scrapy.Field()
    embeds = scrapy.Field()
