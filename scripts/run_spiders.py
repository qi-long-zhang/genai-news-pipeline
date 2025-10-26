#!/usr/bin/env python3
"""
Run every Scrapy spider discovered in this project.
"""

from scrapy.crawler import CrawlerProcess
from scrapy.spiderloader import SpiderLoader
from scrapy.utils.project import get_project_settings


def main():
    settings = get_project_settings()
    loader = SpiderLoader(settings)
    spider_names = sorted(loader.list())

    if not spider_names:
        raise SystemExit("No spiders found in the project.")

    process = CrawlerProcess(settings)
    for name in spider_names:
        print(f"[+] scheduling spider: {name}")
        process.crawl(name)

    process.start()


if __name__ == "__main__":
    main()
