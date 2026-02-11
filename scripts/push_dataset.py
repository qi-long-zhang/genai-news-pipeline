"""
Build a summarization dataset from the 33 topic exemplar articles
and push to HuggingFace Hub.
"""

import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from datasets import Dataset
from huggingface_hub import login

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
HF_TOKEN = os.getenv("HF_TOKEN")
HF_REPO = "zhang-qilong/SG-News-Summarization"

EXEMPLARS_FILE = os.path.join(os.path.dirname(__file__), "..", "topic_exemplars.json")

SOURCE_MAP = {
    "mothership": "Mothership",
    "straits_times": "The Straits Times",
    "channel_news_asia": "Channel News Asia",
}


def main():
    with open(EXEMPLARS_FILE, "r") as f:
        exemplars = json.load(f)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]

    rows = []
    for coll_name, topics in exemplars.items():
        for topic, info in topics.items():
            if not info:
                continue
            url = info["url"]
            doc = db[coll_name].find_one(
                {"article.url": url},
                projection={
                    "article.title": 1,
                    "article.subtitle": 1,
                    "article.content": 1,
                },
            )
            if not doc:
                print(f"WARNING: not found in DB: {url}")
                continue

            art = doc.get("article", {})

            # Build content string from content blocks
            content_blocks = art.get("content") or []
            content_texts = []
            for block in content_blocks:
                text = block.get("text") if isinstance(block, dict) else str(block)
                if text:
                    content_texts.append(text)
            content_str = "\n\n".join(content_texts)

            rows.append(
                {
                    "title": art.get("title") or "",
                    "subtitle": art.get("subtitle") or "",
                    "content": content_str,
                    "source": SOURCE_MAP.get(coll_name, coll_name),
                    "topic": topic,
                }
            )

    client.close()

    print(f"Built dataset with {len(rows)} rows")

    # Show distribution
    from collections import Counter

    print("\nBy source:")
    for src, cnt in Counter(r["source"] for r in rows).items():
        print(f"  {src}: {cnt}")
    print("\nBy topic:")
    for t, cnt in sorted(Counter(r["topic"] for r in rows).items()):
        print(f"  {t}: {cnt}")

    # Create HuggingFace dataset
    dataset = Dataset.from_list(rows)
    print(f"\nDataset: {dataset}")

    # Push to HuggingFace
    login(token=HF_TOKEN)
    dataset.push_to_hub(HF_REPO, private=False)
    print(f"\nPushed to https://huggingface.co/datasets/{HF_REPO}")


if __name__ == "__main__":
    main()
