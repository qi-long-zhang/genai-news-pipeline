import scrapy
from datetime import timedelta, timezone, datetime
import json
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

        self.page = 1
        self.cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)
        self.max_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        self.existing_articles = {}

        with MongoClient(mongo_uri, tz_aware=True) as client:
            collection = client[mongo_db][mongo_collection]
            cursor = collection.find(
                {"article.update_date": {"$gte": self.cutoff_date}},
                projection={"_id": 1, "article.update_date": 1},
            )
            for doc in cursor:
                article_data = doc.get("article", {})
                u_date = article_data.get("update_date")  # UTC
                if u_date:
                    self.existing_articles[doc["_id"]] = u_date

        yield scrapy.Request(
            f"https://www.straitstimes.com/_plat/api/v1/articlesListing?pageType=section&searchParam=singapore&page={self.page}&maxDate={self.max_date}"
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
        articles = data.get("cards") or []
        if not articles:
            return

        for article in articles:
            article = article.get("articleCard")
            if not article:
                continue

            media = article.get("media")
            if (
                not media
                or not media[0].get("image")
                or not media[0]["image"].get("src")
            ):
                continue

            publish_date = _parse_date(article.get("publishedDate"))  # UTC
            if publish_date and publish_date < self.cutoff_date:  # UTC compare
                return

            article_id = article.get("urlPath")
            update_date = _parse_date(article.get("updatedDate"))  # UTC
            if article_id in self.existing_articles:
                existing_update_date = self.existing_articles[article_id]  # UTC
                if update_date == existing_update_date:  # UTC compare
                    continue

            item = NewsArticleItem()
            item["_id"] = article_id
            item["url"] = f"https://www.straitstimes.com{article_id}"
            item["source"] = "The Straits Times"

            item["title"] = article.get("title")
            item["cover_image"] = media[0]["image"]["src"]
            item["images"] = [
                {
                    "url": media[0]["image"]["src"],
                    "caption": media[0]["image"]["caption"],
                    "credit": media[0]["image"]["credit"],
                }
            ]
            item["publish_date"] = publish_date  # UTC
            item["update_date"] = update_date  # UTC

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

        item = response.meta["item"]

        item["subtitle"] = response.css(
            'div[data-testid="headline-stack-test-id"] p.font-body-baseline-regular[data-testid="paragraph-test-id"]::text'
        ).get()

        item["author"] = response.css(
            '[data-testid="masthead-author-byline-test-id"] p.font-eyebrow-lg-bold::text'
        ).get()

        summary_container = response.css('div[data-testid="aisummary-test-id"]')
        if summary_container:
            item["summary"] = summary_container.css("li").xpath("string(.)").getall()

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

        images = item["images"]
        image_nodes = response.css('figure[data-testid="inline-media-test-id"]')
        for node in image_nodes:
            img_url = _clean(node.css("img::attr(src)").get())
            if not img_url:
                continue
            caption = node.css(
                "figcaption p[data-testid='inline-media-caption-test-id']::text"
            ).get()
            credit = _clean(
                node.css(
                    "figcaption p[data-testid='inline-media-credit-test-id']::text"
                ).get()
            )
            image = {"url": img_url, "caption": caption}
            if credit:
                image["credit"] = credit
            images.append(image)
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
            text = node.xpath("string(.)").get()
            if not raw_url or any(ex in raw_url for ex in excluded_substrings):
                continue
            clean_url = response.urljoin(raw_url.split("?")[0])
            is_internal = self.allowed_domains[0] in clean_url
            links.append({"url": clean_url, "text": text, "is_internal": is_internal})
        item["links"] = links

        item["topics"] = response.css(
            'div[data-testid="tags-test-id"] p[data-testid="topic-tag-content-test-id"]::text'
        ).getall()

        yield item
