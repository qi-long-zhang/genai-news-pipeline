import scrapy
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

        query = {"needs_scraping": True, "article_url": {"$exists": True, "$ne": None}}

        with MongoClient(mongo_uri) as client:
            collection = client[mongo_db][mongo_collection]
            for doc in collection.find(query):
                article_url = doc.get("article_url")
                _id = doc.get("_id")
                if not article_url:
                    continue

                yield scrapy.Request(
                    url=article_url,
                    meta={"cloudscraper": True, "_id": _id, "article_url": article_url},
                )

    def parse(self, response):
        def _clean(value):
            return value.strip() if value else None

        def _parse_date(date_str):
            if not date_str:
                return None
            return parser.parse(date_str)

        item = NewsArticleItem()
        item["_id"] = response.meta.get("_id")
        item["article_url"] = response.meta.get("article_url")

        head = response.css("div.article-head")
        item["title"] = _clean(head.css("h1.title::text").get())
        item["subtitle"] = _clean(head.css("p.sub-title::text").get())

        author_time = head.css("div.author-time")
        item["author"] = _clean(
            author_time.css("a[href='#author'].underline::text").get()
        )
        publish_date_str = _clean(author_time.css("div.time h3::text").get())
        item["publish_date"] = _parse_date(publish_date_str)
        item["update_date"] = item["publish_date"]

        content_section = response.css("div.content")

        content = []
        text_nodes = content_section.css(
            ":scope > h2, :scope > h3, :scope > p, :scope > blockquote:not(.instagram-media)"
        )
        for text_node in text_nodes:
            tag = text_node.root.tag
            all_text = text_node.xpath(".//text()[not(ancestor::figure)]").getall()
            text = _clean(" ".join(t.strip() for t in all_text if t.strip()))
            if tag == "h2" and text and "Related" in text:
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
            caption = _clean(img_node.xpath("normalize-space(.//figcaption)").get())
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
            link_text = _clean(link_node.css("::text").get())
            if not link_url or link_url in excluded_links:
                continue
            if "email-protection" in link_url or "[email" in (link_text or ""):
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
