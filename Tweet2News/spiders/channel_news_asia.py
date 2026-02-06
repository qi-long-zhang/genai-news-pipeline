import json
from datetime import datetime, timedelta, timezone

import scrapy
from dateutil import parser
from pymongo import MongoClient
from w3lib.html import remove_tags, replace_entities

from Tweet2News.items import NewsArticleItem


class ChannelNewsAsiaSpider(scrapy.Spider):
    name = "channel_news_asia"
    allowed_domains = ["channelnewsasia.com"]

    async def start(self):
        mongo_uri = self.settings.get("MONGO_URI")
        mongo_db = self.settings.get("MONGO_DATABASE")
        mongo_collection = self.name

        self.page = 0
        self.cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)
        self.existing_articles = {}

        with MongoClient(mongo_uri, tz_aware=True) as client:
            collection = client[mongo_db][mongo_collection]
            cursor = collection.find(
                {"publish_date": {"$gte": self.cutoff_date}},
                projection={"_id": 1, "update_date": 1},
            )
            for doc in cursor:
                u_date = doc.get("update_date")  # UTC
                if u_date:
                    self.existing_articles[doc["_id"]] = u_date

        yield scrapy.Request(
            f"https://www.channelnewsasia.com/api/v1/infinitelisting/424d200d-65b8-46e8-95f6-164946a38f8c?_format=json&viewMode=infinite_scroll_listing&page={self.page}"
        )

    def parse(self, response):
        def _parse_date(date_str):
            if not date_str:
                return None
            dt = parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            return dt.astimezone(timezone.utc)

        data = json.loads(response.text)
        articles = data.get("result") or []

        for article in articles:
            if article.get("type") != "article":
                continue

            date = _parse_date(article.get("date"))  # UTC
            if date and date < self.cutoff_date:  # UTC compare
                return

            article_id = article.get("uuid")
            if article_id in self.existing_articles:
                existing_update_date = self.existing_articles[article_id]  # UTC
                if date == existing_update_date:  # UTC compare
                    continue

            item = NewsArticleItem()
            item["_id"] = article_id
            item["url"] = article.get("absolute_url")

            item["title"] = article.get("title")
            description = article.get("description") or ""
            item["subtitle"] = remove_tags(replace_entities(description))
            summary = article.get("fast", {}).get("tldr_for_shorts", [])
            if summary:
                item["summary"] = scrapy.Selector(text=summary).css("li::text").getall()

            author_details = article.get("cnar_author_details") or []
            item["author"] = (
                ", ".join(
                    cleaned
                    for a in author_details
                    if (cleaned := a.get("author").strip())
                )
                or None
            )

            item["cover_image"] = article.get("img_extra", {}).get("original")

            yield scrapy.Request(
                item["url"],
                self.parse_article,
                meta={"cloudscraper": True, "item": item},
            )

        next_page = self.page + 1
        next_url = response.url.replace(f"page={self.page}", f"page={next_page}")
        self.page = next_page
        yield scrapy.Request(next_url, self.parse)

    def parse_article(self, response):
        def _clean(value):
            return value.strip() if value else None

        def _parse_date(date_str):
            if not date_str:
                return None
            dt = parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            return dt.astimezone(timezone.utc)

        item = response.meta["item"]

        content_section = response.css("div.content")

        article_publish = content_section.css(".article-publish")
        publish_date = article_publish.css("::text").get()
        item["publish_date"] = _parse_date(_clean(publish_date))  # UTC
        item["update_date"] = item["publish_date"]  # UTC
        update_date = article_publish.css("span::text").get()
        if update_date:
            cleaned_update_date = _clean(
                update_date.replace("(Updated:", "").replace(")", "")
            )
            item["update_date"] = _parse_date(cleaned_update_date)  # UTC

        content = []
        content_nodes = content_section.xpath(
            ".//*[self::div[contains(@class, 'content-detail__description')] or "
            "self::div[contains(@class, 'text-long')]]"
            "//*[self::p or self::h2 or self::li or (self::blockquote and not(contains(@class, 'instagram-media')))]"
            "[not(ancestor::div[contains(@class, 'context-snippet')])]"
            "[not(ancestor::div[contains(@class, 'title-block')])]"
            "[not(ancestor::blockquote[contains(@class, 'instagram-media')])]"
        )
        for node in content_nodes:
            tag = node.root.tag
            text = node.xpath("string(.)").get()
            if text:
                content.append({"type": tag, "text": text})
        item["content"] = content

        images = []
        image_nodes = content_section.css("figure")
        for img_node in image_nodes:
            img_url = img_node.css("img::attr(src)").get()
            caption = img_node.xpath("normalize-space(.//figcaption)").get()
            if img_url:
                images.append({"url": img_url, "caption": caption})
        item["images"] = images

        videos = []
        youtube_nodes = content_section.css(
            "iframe[src*='youtube.com'], iframe[src*='youtu.be']"
        )
        for vid_node in youtube_nodes:
            vid_url = vid_node.css("::attr(src)").get()
            if vid_url:
                videos.append(vid_url)
        brightcove_nodes = content_section.css("video-js")
        for bc_node in brightcove_nodes:
            data_account = _clean(bc_node.css("::attr(data-account)").get())
            data_player = _clean(bc_node.css("::attr(data-player)").get())
            data_video_id = _clean(bc_node.css("::attr(data-video-id)").get())
            if data_account and data_player and data_video_id:
                bc_url = (
                    f"https://players.brightcove.net/{data_account}/"
                    f"{data_player}_default/index.html?videoId={data_video_id}"
                )
                videos.append(bc_url)
        item["videos"] = videos

        item["source"] = "CNA"
        source = content_section.css(".source.source--with-label::text").get()
        if source:
            source.replace("Source:", "")

        links = []
        for node in content_nodes:
            a_nodes = node.css("a")
            for a_node in a_nodes:
                raw_url = _clean(a_node.css("::attr(href)").get())
                text = a_node.xpath("string(.)").get()
                if not raw_url or raw_url.startswith(("javascript:", "mailto:", "#")):
                    continue
                clean_url = response.urljoin(raw_url.split("?")[0])
                is_internal = self.allowed_domains[0] in clean_url
                links.append(
                    {"url": clean_url, "text": text, "is_internal": is_internal}
                )
        item["links"] = links

        embeds = []
        embed_nodes = content_section.css("blockquote.instagram-media")
        for embed_node in embed_nodes:
            embed_url = embed_node.css("::attr(data-instgrm-permalink)").get()
            if embed_url:
                embeds.append(embed_url)
        item["embeds"] = embeds

        item["topics"] = content_section.css(
            "[data-title='Related Topics'] a::text"
        ).getall()

        yield item
