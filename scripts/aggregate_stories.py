import os
import numpy as np
import networkx as nx
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")
if "channel_news_asia" not in MONGO_COLLECTIONS:
    MONGO_COLLECTIONS.append("channel_news_asia")

# Thresholds
SIMILARITY_THRESHOLD = 0.9  # For merging into existing stories or clustering new ones
HOT_STORIES_COLLECTION = "hot_stories"
NEW_ARTICLE_WINDOW_HOURS = 24  # Look back 24 hours for new articles
STORY_RETENTION_HOURS = 24  # Keep articles in a story for 24 hours


def get_active_stories(db):
    """
    Fetch all active stories (those still in their update lifecycle).
    """
    collection = db[HOT_STORIES_COLLECTION]
    # Fetching all where is_active is True to allow merging
    cursor = collection.find({"is_active": True})
    return list(cursor)


def get_new_articles(db):
    """
    Fetch all new articles from all collections within the NEW_ARTICLE_WINDOW_HOURS.
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(hours=NEW_ARTICLE_WINDOW_HOURS)
    new_articles = []

    for coll_name in MONGO_COLLECTIONS:
        cursor = db[coll_name].find({"article.publish_date": {"$gte": cutoff_date}})
        new_articles.extend(list(cursor))

    return new_articles


def calculate_cosine_similarity(vec1, vec2):
    """
    Calculate cosine similarity between two vectors.
    """
    if vec1 is None or vec2 is None:
        return 0
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    if vec1.size == 0 or vec2.size == 0:
        return 0
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    if norm1 == 0 or norm2 == 0:
        return 0
    return np.dot(vec1, vec2) / (norm1 * norm2)


def get_article_ref(art):
    """
    Extract necessary fields for ref_articles.
    """
    article = art.get("article", {})
    return {
        "url": article.get("article_url"),
        "title": article.get("title"),
        "subtitle": article.get("subtitle") or "",
        "summary": article.get("summary") or "",
        "author": article.get("author") or "",
        "cover_image": article.get("cover_image") or art.get("cover_image"),
        "publish_date": article.get("publish_date"),
        "update_date": article.get("update_date"),
        "content": article.get("content"),
        "source": article.get("source") or art.get("source"),
        "embedding": art.get("embedding", {}).get("vector"),
        "is_popular": art.get("prediction", {}).get("label") == "popular",
    }


def check_is_visible(ref_articles):
    """
    Evaluate if a story should be visible.
    Criteria: 1) Has a popular article OR 2) Has more than or equal to 3 articles.
    """
    has_popular = any(ref.get("is_popular") for ref in ref_articles)
    return has_popular or len(ref_articles) >= 3


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]

    # 1. Get currently active stories (lifecycle: True)
    active_stories = get_active_stories(db)

    # 2. Get new articles to process
    new_articles = get_new_articles(db)

    # 3. Filter and deduplicate new articles
    existing_article_urls = set()
    for story in active_stories:
        for article_ref in story.get("ref_articles", []):
            existing_article_urls.add(article_ref["url"])

    new_articles = [
        art
        for art in new_articles
        if art.get("article", {}).get("article_url") not in existing_article_urls
    ]

    # 4. Try to merge new articles into existing active stories
    remaining_articles = []
    merged_count = 0

    for art in new_articles:
        art_embedding = art.get("embedding", {}).get("vector")
        if not art_embedding:
            continue

        found_match = False
        for story in active_stories:
            for ref in story.get("ref_articles", []):
                ref_embedding = ref.get("embedding")
                if (
                    calculate_cosine_similarity(art_embedding, ref_embedding)
                    >= SIMILARITY_THRESHOLD
                ):
                    story.setdefault("ref_articles", []).append(get_article_ref(art))
                    story["is_updated"] = True
                    story["is_content_updated"] = True
                    found_match = True
                    merged_count += 1
                    break
            if found_match:
                break

        if not found_match:
            remaining_articles.append(art)

    # 5. Cluster remaining articles using chain aggregation
    if remaining_articles:
        G = nx.Graph()
        for i in range(len(remaining_articles)):
            G.add_node(i)

        for i in range(len(remaining_articles)):
            emb_i = remaining_articles[i].get("embedding", {}).get("vector")
            if not emb_i:
                continue
            for j in range(i + 1, len(remaining_articles)):
                emb_j = remaining_articles[j].get("embedding", {}).get("vector")
                if not emb_j:
                    continue
                if calculate_cosine_similarity(emb_i, emb_j) >= SIMILARITY_THRESHOLD:
                    G.add_edge(i, j)

        new_stories_to_insert = []
        for component in nx.connected_components(G):
            component_articles = [remaining_articles[idx] for idx in component]

            ref_articles = [get_article_ref(art) for art in component_articles]
            is_visible = check_is_visible(ref_articles)

            # Save all clusters of 2+ articles, or 1 if it's popular
            if len(ref_articles) >= 2 or is_visible:
                new_story = {
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                    "is_active": True,
                    "is_visible": is_visible,
                    "ref_articles": ref_articles,
                    "summarization": None,
                }
                new_stories_to_insert.append(new_story)

        if new_stories_to_insert:
            db[HOT_STORIES_COLLECTION].insert_many(new_stories_to_insert)
            print(f"Created {len(new_stories_to_insert)} new stories.")

    # 6. Lifecycle Management: Deactivate expired stories
    retention_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=STORY_RETENTION_HOURS
    )
    deactivated_count = 0
    for story in active_stories:
        latest_upd_date = None
        for ref in story.get("ref_articles", []):
            upd_date = ref.get("update_date")
            if upd_date:
                if latest_upd_date is None or upd_date > latest_upd_date:
                    latest_upd_date = upd_date

        if latest_upd_date and latest_upd_date < retention_cutoff:
            story["is_active"] = False
            story["is_updated"] = True
            deactivated_count += 1

    # 7. Final Sync: Update existing stories
    bulk_updates = []
    for story in active_stories:
        if story.get("is_updated"):
            new_visibility = check_is_visible(story["ref_articles"])

            update_data = {
                "ref_articles": story["ref_articles"],
                "updated_at": datetime.now(timezone.utc),
                "is_active": story.get("is_active", True),
                "is_visible": new_visibility,
            }

            # Reset summary if visibility status changed to True OR new articles added while visible
            if new_visibility and story.get("is_content_updated"):
                update_data["summarization"] = None

            bulk_updates.append(UpdateOne({"_id": story["_id"]}, {"$set": update_data}))

    if bulk_updates:
        db[HOT_STORIES_COLLECTION].bulk_write(bulk_updates)
        print(
            f"Updated {len(bulk_updates)} existing stories (Deactivated {deactivated_count})."
        )

    client.close()


if __name__ == "__main__":
    main()
