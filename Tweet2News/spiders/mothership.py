import json
import scrapy
from datetime import timedelta, timezone, datetime
from dateutil import parser
from pymongo import MongoClient
from urllib.parse import urljoin

from Tweet2News.items import NewsArticleItem


class MothershipSpider(scrapy.Spider):
    name = "mothership"
    allowed_domains = ["mothership.sg"]

    async def start(self):
        mongo_uri = self.settings.get("MONGO_URI")
        mongo_db = self.settings.get("MONGO_DATABASE")
        mongo_collection = self.name

        self.page = 1
        self.cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)
        self.existing_articles = {}

        with MongoClient(mongo_uri, tz_aware=True) as client:
            collection = client[mongo_db][mongo_collection]
            cursor = collection.find(
                {"article.publish_date": {"$gte": self.cutoff_date}},
                projection={
                    "_id": 1,
                    "article.publish_date": 1,
                    "article.update_date": 1,
                },
            )
            for doc in cursor:
                article_data = doc.get("article", {})
                p_date = article_data.get("publish_date")  # UTC
                u_date = article_data.get("update_date")  # UTC
                if p_date and u_date:
                    self.existing_articles[doc["_id"]] = {
                        "publish_date": p_date,
                        "update_date": u_date,
                    }

        yield scrapy.Request(
            url=f"https://mothership.sg/json/posts-{self.page}.json",
            headers={"Referer": "https://mothership.sg/"},
            meta={"cloudscraper": True},
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
        if not data:
            return

        for article in data:
            date = _parse_date(article.get("date"))  # UTC
            if date and date < self.cutoff_date:  # UTC compare
                return

            article_id = article.get("name")
            if article_id in self.existing_articles:
                existing_update_date = self.existing_articles[article_id][
                    "update_date"
                ]  # UTC
                if date == existing_update_date:  # UTC compare
                    continue

            item = NewsArticleItem()
            item["_id"] = article_id
            item["url"] = article.get("url")
            item["source"] = "Mothership"

            item["title"] = article.get("title")
            item["subtitle"] = article.get("excerpt")
            item["cover_image"] = article.get("image_url")
            if article_id in self.existing_articles:
                item["publish_date"] = self.existing_articles[article_id][
                    "publish_date"
                ]
            else:
                item["publish_date"] = date  # UTC
            item["update_date"] = date  # UTC

            yield scrapy.Request(
                item["url"],
                self.parse_article,
                meta={"cloudscraper": True, "item": item},
            )

        next_page = self.page + 1
        next_url = response.url.replace(
            f"posts-{self.page}.json", f"posts-{next_page}.json"
        )
        self.page = next_page
        yield scrapy.Request(next_url, self.parse)

    def parse_article(self, response):
        def _clean(value):
            return value.strip() if value else None

        item = response.meta["item"]

        item["author"] = response.css(
            "div.article-head div.author-time a[href='#author'].underline::text"
        ).get()

        content_section = response.css("div.content")

        content = []
        text_nodes = content_section.css(
            ":scope > h2, :scope > h3, :scope > p, :scope > blockquote:not(.instagram-media):not(.tiktok-embed)"
        )
        for text_node in text_nodes:
            tag = text_node.root.tag
            all_text = text_node.xpath(".//text()[not(ancestor::figure)]").getall()
            text = " ".join(cleaned for t in all_text if (cleaned := _clean(t)))
            if tag in {"h2", "h3"} and text and "Related" in text:
                continue
            if text:
                content.append({"tag": tag, "text": text})
        item["content"] = content

        images = []
        featured_image_url = _clean(
            response.css("div.image.featured img::attr(src)").get()
        )
        if featured_image_url:
            images.append({"url": featured_image_url, "caption": ""})

        image_nodes = content_section.css("figure")
        for img_node in image_nodes:
            img_url = _clean(img_node.css("img::attr(src)").get())
            caption = img_node.xpath("normalize-space(.//figcaption)").get()
            if img_url:
                images.append({"url": img_url, "caption": caption})
        item["images"] = images

        videos = []
        video_nodes = content_section.css(
            "iframe[src*='youtube.com'], iframe[src*='youtu.be']"
        )
        for vid_node in video_nodes:
            vid_url = _clean(vid_node.css("::attr(src)").get())
            if vid_url:
                videos.append(vid_url)
        item["videos"] = videos

        excluded_links = {"https://bit.ly/3qgqzHg", "https://bit.ly/3KjTj94"}
        links = []
        link_nodes = content_section.css("a[href]")
        for link_node in link_nodes:
            link_url = _clean(link_node.css("::attr(href)").get())
            link_text = _clean(link_node.css("::text").get()) or ""
            if not link_url or link_url in excluded_links:
                continue
            if "email-protection" in link_url or "[email" in link_text:
                continue
            is_internal = (
                link_url.startswith("/") or self.allowed_domains[0] in link_url
            )
            links.append(
                {"url": link_url, "text": link_text, "is_internal": is_internal}
            )
        item["links"] = links

        embeds = []
        embed_nodes = content_section.css("iframe[src^='/']")
        for embed_node in embed_nodes:
            embed_url = _clean(embed_node.css("::attr(src)").get())
            if embed_url:
                embeds.append(urljoin(response.url, embed_url))
        item["embeds"] = embeds

        yield item
