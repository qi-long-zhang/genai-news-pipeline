import scrapy
from datetime import timedelta, timezone
from dateutil import parser
from pymongo import MongoClient
from urllib.parse import urljoin

from genai_news_pipeline.items import NewsArticleItem


class MothershipSpider(scrapy.Spider):
    name = "mothership"
    allowed_domains = ["mothership.sg"]

    @staticmethod
    def _clean(value):
        return value.strip() if value else None

    async def start(self):
        mongo_uri = self.settings.get("MONGO_URI")
        mongo_db = self.settings.get("MONGO_DATABASE")
        mongo_collection = self.name

        with MongoClient(mongo_uri, tz_aware=True) as client:
            collection = client[mongo_db][mongo_collection]

            latest_doc = collection.find_one(
                projection={"article.publish_date": 1},
                sort=[("article.publish_date", -1)],
            )
            if latest_doc and (
                latest_publish_date := latest_doc.get("article", {}).get("publish_date")
            ):
                self.cutoff_date = latest_publish_date + timedelta(seconds=1)

        yield scrapy.Request(
            url="https://mothership.sg/",
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

        articles = response.css("article[data-post-id]")
        if not articles:
            return

        for article in articles:
            date = _parse_date(
                self._clean(article.css("span.meta-time::text").get())
            )  # UTC
            if date and date < self.cutoff_date:
                return

            url = (
                article.css("h2.post-title a::attr(href)").get()
                or article.css("div.image-box a::attr(href)").get()
            )
            if not url:
                continue

            _id = self._clean(article.attrib.get("data-post-id"))
            if not _id:
                continue

            item = NewsArticleItem()
            item["_id"] = _id
            item["url"] = url
            item["source"] = "Mothership"

            item["title"] = self._clean(article.css("h2.post-title a::text").get())
            item["cover_image"] = article.css("div.image-box img::attr(src)").get()
            item["publish_date"] = date  # UTC
            item["update_date"] = date  # UTC

            yield scrapy.Request(
                url,
                self.parse_article,
                meta={"cloudscraper": True, "item": item},
            )

    def parse_article(self, response):
        item = response.meta["item"]

        item["subtitle"] = self._clean(
            response.css("div.article-head p.sub-title::text").get()
        )

        item["author"] = self._clean(
            response.css(
                "div.article-head div.author-time a[href='#author'].underline::text"
            ).get()
        )

        content_section = response.css("div.content")

        content = []
        text_nodes = content_section.css(
            ":scope > h2, :scope > h3, :scope > p, :scope > blockquote:not(.instagram-media):not(.tiktok-embed)"
        )
        for text_node in text_nodes:
            tag = text_node.root.tag
            all_text = text_node.xpath(".//text()[not(ancestor::figure)]").getall()
            text = " ".join(cleaned for t in all_text if (cleaned := self._clean(t)))
            if tag in {"h2", "h3"} and text and "Related" in text:
                continue
            if text:
                content.append({"tag": tag, "text": text})
        item["content"] = content

        images = []
        featured_image_url = self._clean(
            response.css("div.image.featured img::attr(src)").get()
        )
        if featured_image_url:
            images.append({"url": featured_image_url, "caption": ""})

        image_nodes = content_section.css("figure")
        for img_node in image_nodes:
            img_url = self._clean(img_node.css("img::attr(src)").get())
            caption = img_node.xpath("normalize-space(.//figcaption)").get()
            if img_url:
                images.append({"url": img_url, "caption": caption})
        item["images"] = images

        videos = []
        video_nodes = content_section.css(
            "iframe[src*='youtube.com'], iframe[src*='youtu.be']"
        )
        for vid_node in video_nodes:
            vid_url = self._clean(vid_node.css("::attr(src)").get())
            if vid_url:
                videos.append(vid_url)
        item["videos"] = videos

        excluded_links = {"https://bit.ly/3qgqzHg", "https://bit.ly/3KjTj94"}
        links = []
        link_nodes = content_section.css("a[href]")
        for link_node in link_nodes:
            link_url = self._clean(link_node.css("::attr(href)").get())
            link_text = self._clean(link_node.css("::text").get())
            if not link_url or link_url in excluded_links or not link_text:
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
            embed_url = self._clean(embed_node.css("::attr(src)").get())
            if embed_url:
                embeds.append(urljoin(response.url, embed_url))
        item["embeds"] = embeds

        yield item
