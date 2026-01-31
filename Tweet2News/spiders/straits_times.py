import scrapy
from datetime import timedelta, timezone
from dateutil import parser
from pymongo import MongoClient

from Tweet2News.items import NewsArticleItem


class StraitsTimesSpider(scrapy.Spider):
    name = "straits_times"
    allowed_domains = ["straitstimes.com"]

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
            return value.replace("\xa0", " ").strip() if value else None

        def _parse_date(date_str):
            if not date_str:
                return None
            dt = parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            return dt.astimezone(timezone.utc)

        item = NewsArticleItem()
        item["_id"] = response.meta.get("_id")
        item["article_url"] = response.meta.get("article_url")

        item["title"] = _clean(
            response.css('h1[data-testid="heading-test-id"]::text').get()
        )

        item["author"] = _clean(
            response.css(
                '[data-testid="masthead-author-byline-test-id"] p.font-eyebrow-lg-bold::text'
            ).get()
        )

        timestamp_elements = response.css('div[data-testid="timestamp-test-id"]')
        for element in timestamp_elements:
            raw_text = "".join(element.css("p::text").getall())
            if "Published" in raw_text:
                item["publish_date"] = _parse_date(
                    _clean(raw_text.replace("Published", ""))
                )  # UTC
                item["update_date"] = item["publish_date"]  # UTC
            elif "Updated" in raw_text:
                item["update_date"] = _parse_date(
                    _clean(raw_text.replace("Updated", ""))
                )  # UTC

        summary_container = response.css('div[data-testid="aisummary-test-id"]')
        if summary_container:
            raw_bullets = summary_container.css("li").xpath("string(.)").getall()
            summary_list = [_clean(t) for t in raw_bullets if _clean(t)]
            item["summary"] = summary_list if summary_list else None
        else:
            item["summary"] = None

        content_nodes = response.css(
            'p[data-testid="article-paragraph-annotation-test-id"], '
            'h2[data-testid="article-subhead-test-id"]'
        )
        content = []
        for node in content_nodes:
            tag = node.xpath("name()").get()
            text = _clean(node.xpath("string(.)").get())
            if text:
                content.append({"tag": tag, "text": text})
        item["content"] = content

        images = []
        image_nodes = response.css(
            'div[data-testid="article-hero-media-test-id"], '
            'figure[data-testid="inline-media-test-id"]'
        )
        for node in image_nodes:
            img_url = _clean(
                (
                    node.css("img::attr(src)").get()
                    or (
                        response.css('meta[property="og:image"]::attr(content)').get()
                        if node.attrib.get("data-testid")
                        == "article-hero-media-test-id"
                        else None
                    )
                )
            )
            if not img_url:
                continue
            caption_list = []
            for cap in node.css(".hero-media-caption p, figcaption p"):
                text = _clean(cap.xpath("string(.)").get())
                if text:
                    caption_list.append(text)
            images.append({"url": img_url, "caption": caption_list})
        item["images"] = images

        videos = []
        video_frames = response.css(
            'div[data-testid="social-media-embed-test-id"] iframe'
        )
        for frame in video_frames:
            src = _clean(frame.css("::attr(src)").get())
            if src and ("youtube.com" in src or "youtu.be" in src):
                videos.append(src)
        item["videos"] = videos

        links = []
        link_nodes = response.css(
            'p[data-testid="article-paragraph-annotation-test-id"] a[href], '
            'h2[data-testid="article-subhead-test-id"] a[href]'
        )
        excluded_substrings = ["newsletter-signup", "headstart-signup"]
        for node in link_nodes:
            raw_url = _clean(node.css("::attr(href)").get())
            text = _clean(node.xpath("string(.)").get())
            if not raw_url or any(ex in raw_url for ex in excluded_substrings):
                continue
            clean_url = response.urljoin(raw_url.split("?")[0])
            is_internal = self.allowed_domains[0] in clean_url
            links.append({"url": clean_url, "text": text, "is_internal": is_internal})
        item["links"] = links

        topics = []
        topic_nodes = response.css(
            'div[data-testid="tags-test-id"] button[data-testid="button-test-id"]'
        )
        for node in topic_nodes:
            topic_text = _clean(node.xpath("string(.)").get())
            if topic_text:
                topics.append(topic_text)
        item["topics"] = topics

        yield item
