import os
import json
import time
import re
import numpy as np
import networkx as nx
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from google import genai

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PROMPTS_FILE = "data/json/prompts_production.json"
MODEL_ID = "gemini-3-pro-preview"
SUMMARY_API_MAX_RETRIES = 3
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
    Returns articles with their collection name for tracking.
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(hours=NEW_ARTICLE_WINDOW_HOURS)
    new_articles = []

    for coll_name in MONGO_COLLECTIONS:
        cursor = db[coll_name].find({"article.publish_date": {"$gte": cutoff_date}})
        for art in cursor:
            art["_collection_name"] = coll_name  # Track which collection this came from
            new_articles.append(art)

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
    Extract necessary fields for ref_articles to be stored in stories.
    """
    article = art.get("article", {})
    return {
        "article_id": art.get("_id"),
        "collection": art.get("_collection_name"),  # Store for efficient lookups
        "url": article.get("url"),
        "source": article.get("source"),
        "title": article.get("title"),
        "cover_image": article.get("cover_image"),
        "update_date": article.get("update_date"),
        "is_popular": art.get("prediction", {}).get("label") == "popular",
        "score": art.get("prediction", {}).get("score", 0),
    }


def check_is_visible(ref_articles):
    """
    Evaluate if a story should be visible.
    Criteria: 1) Has a popular article OR 2) Has more than or equal to 2 articles.
    """
    has_popular = any(ref.get("is_popular") for ref in ref_articles)
    return has_popular or len(ref_articles) >= 2


def get_latest_ref_article_at(ref_articles):
    """
    Get the latest update_date from ref_articles.
    """
    latest_upd_date = None
    for ref in ref_articles or []:
        upd_date = ref.get("update_date")
        if upd_date and (latest_upd_date is None or upd_date > latest_upd_date):
            latest_upd_date = upd_date
    return latest_upd_date


def get_cover_images(ref_articles):
    """
    Build ordered cover_images from ref_articles.
    """
    return [ref.get("cover_image") for ref in (ref_articles or [])]


def load_prompt_template(prompt_key):
    """
    Load production prompt template.
    """
    with open(PROMPTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return (data.get("prompts") or {}).get(prompt_key)


def extract_final_summary(text):
    """
    Extract summary from LLM output by locating the final "Final Summary" marker.
    Supports both "Final Summary" and "Final Summary:".
    """
    if not text:
        return ""

    parts = re.split(r"(?:^|\n)\s*Final Summary\s*:?\s*", text, flags=re.IGNORECASE)
    if len(parts) > 1:
        return parts[-1].strip()
    return text.strip()


def extract_final_headline(text):
    """
    Extract headline from Meta Prompting output using the
    '**Final Headline:**' marker format.
    """
    if not text:
        return ""

    marker = "Final Headline:"
    if marker not in text:
        return ""

    headline = text.split(marker, 1)[1].strip()
    headline = headline.splitlines()[0].strip() if headline else ""

    return headline


def build_empty_summary_message(response):
    """
    Build a readable fallback message when model output is empty.
    """
    block_reason = getattr(
        getattr(response, "prompt_feedback", None), "block_reason", None
    )
    if block_reason:
        reason = getattr(block_reason, "value", None) or str(block_reason)
        return f"Summary unavailable: model response blocked ({reason})."

    return "Summary unavailable: model returned empty content."


def cache_source_article(source_article, collection_name, source_article_cache):
    """
    Cache a source article by (collection, article_id).
    """
    article_id = source_article["_id"]
    source_article_cache[(collection_name, article_id)] = source_article


def build_source_article_cache(db, stories):
    """
    Batch-fetch source articles for all refs in active stories.
    """
    ids_by_collection = {}
    source_article_cache = {}

    for story in stories:
        for ref in story.get("ref_articles", []):
            ids_by_collection.setdefault(ref["collection"], set()).add(
                ref["article_id"]
            )

    for coll_name, article_ids in ids_by_collection.items():
        cursor = db[coll_name].find({"_id": {"$in": list(article_ids)}})
        for source_article in cursor:
            cache_source_article(source_article, coll_name, source_article_cache)

    return source_article_cache


def get_source_article_for_ref(ref_article, source_article_cache):
    """
    Resolve a source article from cache using (collection, article_id).
    """
    coll_name = ref_article["collection"]
    article_id = ref_article["article_id"]
    return source_article_cache[(coll_name, article_id)]


def get_embedding_for_ref(ref_article, source_article_cache):
    """
    Resolve embedding from source article cache.
    """
    source_article = get_source_article_for_ref(ref_article, source_article_cache)
    return source_article["embedding"]["vector"]


def extract_article_content(source_article):
    """
    Convert article content into plain text.
    """
    content_blocks = source_article.get("article", {}).get("content") or []
    content_texts = []
    for block in content_blocks:
        if block is None:
            continue
        text = block.get("text") if isinstance(block, dict) else str(block)
        if text:
            content_texts.append(text)

    return "\n\n".join(content_texts).strip()


def format_prompt(ref_articles, template, source_article_cache):
    """
    Format prompt template with content only.
    """
    content_parts = []
    for article_ref in ref_articles:
        source_article = get_source_article_for_ref(article_ref, source_article_cache)
        article_content = extract_article_content(source_article)
        if article_content:
            content_parts.append(article_content)

    content = "\n\n".join(content_parts).strip()
    return template.format(content=content)


def generate_content_with_retry(genai_client, prompt, story_id):
    """
    Call Gemini API with retry logic. Retries up to SUMMARY_API_MAX_RETRIES times.
    """
    for attempt in range(1, SUMMARY_API_MAX_RETRIES + 1):
        try:
            return genai_client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
            )
        except Exception as e:
            if attempt >= SUMMARY_API_MAX_RETRIES:
                raise
            print(
                f"Warning: Summarize API call failed for story {story_id} "
                f"(attempt {attempt}/{SUMMARY_API_MAX_RETRIES}): {e}. Retrying..."
            )
            time.sleep(attempt)


def summarize_story(
    story,
    genai_client,
    single_prompt_template,
    multi_prompt_template,
    source_article_cache,
):
    """
    Generate story summary/headline.
    Single article: headline is article title, summary generated by LLM when missing.
    Multi article: headline and summary are generated by LLM when missing.
    """
    ref_articles = story.get("ref_articles") or []
    if not ref_articles:
        return

    is_multi_article = len(ref_articles) > 1
    if is_multi_article:
        ref_articles.sort(
            key=lambda ref: ref.get("update_date"),
            reverse=True,
        )
    default_headline = ref_articles[0].get("title") or ""
    story["headline"] = default_headline
    prompt_template = (
        multi_prompt_template if is_multi_article else single_prompt_template
    )
    try:
        prompt = format_prompt(ref_articles, prompt_template, source_article_cache)
        response = generate_content_with_retry(genai_client, prompt, story.get("_id"))
        response_text = (getattr(response, "text", "") or "").strip()
        if not response_text:
            story["summary"] = build_empty_summary_message(response)
            return

        if is_multi_article:
            story["headline"] = extract_final_headline(response_text)
        story["summary"] = extract_final_summary(response_text)

    except Exception as e:
        story_id = story.get("_id")
        print(f"Warning: Failed to summarize story {story_id}: {e}")


def update_ref_articles_from_source(db, active_stories, source_article_cache):
    """
    Update ref_articles in active stories if the source article has needs_aggregation=True.
    Returns a list of source articles to mark as aggregated (needs_aggregation=False).
    """
    ids_by_collection = {}
    updated_lookup = {}

    for story in active_stories:
        for ref_article in story.get("ref_articles", []):
            ids_by_collection.setdefault(ref_article["collection"], set()).add(
                ref_article["article_id"]
            )
    for coll_name in ids_by_collection:
        article_ids = list(ids_by_collection[coll_name])
        query = {"needs_aggregation": True, "_id": {"$in": article_ids}}
        cursor = db[coll_name].find(query)
        for source_article in cursor:
            cache_source_article(source_article, coll_name, source_article_cache)
            article_id = source_article["_id"]
            updated_lookup[(coll_name, article_id)] = source_article

    updated_count = 0
    articles_to_mark = []  # Store (collection_name, article_id) tuples

    for story in active_stories:
        ref_articles = story.get("ref_articles", [])
        updated = False

        for i, ref_article in enumerate(ref_articles):
            coll_name = ref_article["collection"]
            article_id = ref_article["article_id"]
            source_article = updated_lookup.get((coll_name, article_id))
            if source_article:
                # Update the ref_article with latest data
                source_article["_collection_name"] = (
                    coll_name  # Restore collection name
                )
                ref_articles[i] = get_article_ref(source_article)
                updated = True
                # Record for later bulk update
                articles_to_mark.append((coll_name, source_article["_id"]))

        if updated:
            story["ref_articles"] = ref_articles
            story["is_updated"] = True
            story["summary"] = None
            story["headline"] = None
            updated_count += 1

    if updated_count > 0:
        print(
            f"Prepared {updated_count} stories for update with {len(articles_to_mark)} ref_articles from source collections."
        )

    return articles_to_mark


def main():
    client = MongoClient(MONGO_URI, tz_aware=True)
    db = client[MONGO_DATABASE]
    genai_client = genai.Client(api_key=GEMINI_API_KEY)
    single_prompt_template = load_prompt_template("single")
    multi_prompt_template = load_prompt_template("multi")
    has_db_operation = False

    # 1. Get currently active stories (lifecycle: True)
    active_stories = get_active_stories(db)
    source_article_cache = build_source_article_cache(db, active_stories)

    # 2. Update ref_articles from source collections where needs_aggregation=True
    articles_to_mark = update_ref_articles_from_source(
        db, active_stories, source_article_cache
    )

    # 3. Get new articles to process
    new_articles = get_new_articles(db)

    # 4. Filter and deduplicate new articles
    existing_urls = set()
    for story in active_stories:
        for article_ref in story.get("ref_articles", []):
            existing_urls.add(article_ref["url"])

    new_articles = [
        art
        for art in new_articles
        if art.get("article", {}).get("url") not in existing_urls
    ]

    # 5. Try to merge new articles into existing active stories
    remaining_articles = []

    for art in new_articles:
        art_embedding = art.get("embedding", {}).get("vector")
        if not art_embedding:
            continue

        found_match = False
        for story in active_stories:
            for ref in story.get("ref_articles", []):
                ref_embedding = get_embedding_for_ref(ref, source_article_cache)
                if (
                    calculate_cosine_similarity(art_embedding, ref_embedding)
                    >= SIMILARITY_THRESHOLD
                ):
                    cache_source_article(
                        art, art.get("_collection_name"), source_article_cache
                    )
                    story.setdefault("ref_articles", []).append(get_article_ref(art))
                    story["is_updated"] = True
                    story["summary"] = None
                    story["headline"] = None
                    # Track article for needs_aggregation update
                    articles_to_mark.append((art["_collection_name"], art["_id"]))
                    found_match = True
                    break
            if found_match:
                break

        if not found_match:
            remaining_articles.append(art)

    # 6. Cluster remaining articles using chain aggregation
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
            for article in component_articles:
                cache_source_article(
                    article, article.get("_collection_name"), source_article_cache
                )

            ref_articles = [get_article_ref(art) for art in component_articles]
            is_visible = check_is_visible(ref_articles)

            # Save all clusters that are visible
            if is_visible:
                new_story = {
                    "created_at": datetime.now(timezone.utc),  # UTC
                    "updated_at": datetime.now(timezone.utc),  # UTC
                    "latest_ref_article_at": get_latest_ref_article_at(ref_articles),
                    "is_active": True,
                    "is_visible": is_visible,
                    "ref_articles": ref_articles,
                    "cover_images": get_cover_images(ref_articles),
                    "summary": None,
                    "headline": None,
                }

                summarize_story(
                    new_story,
                    genai_client=genai_client,
                    single_prompt_template=single_prompt_template,
                    multi_prompt_template=multi_prompt_template,
                    source_article_cache=source_article_cache,
                )
                new_stories_to_insert.append(new_story)

                # Track articles for needs_aggregation update
                for art in component_articles:
                    articles_to_mark.append((art["_collection_name"], art["_id"]))

        if new_stories_to_insert:
            db[HOT_STORIES_COLLECTION].insert_many(new_stories_to_insert)
            print(f"Created {len(new_stories_to_insert)} new stories.")
            has_db_operation = True

    # 7. Lifecycle Management: Deactivate expired stories
    retention_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=STORY_RETENTION_HOURS
    )
    deactivated_count = 0
    for story in active_stories:
        latest_upd_date = get_latest_ref_article_at(story.get("ref_articles"))

        if latest_upd_date and latest_upd_date < retention_cutoff:
            story["is_active"] = False
            story["is_updated"] = True
            deactivated_count += 1

    # 8. Final Sync: Update existing stories
    bulk_updates = []
    for story in active_stories:
        if story.get("is_updated"):
            new_visibility = check_is_visible(story["ref_articles"])

            if new_visibility:
                summarize_story(
                    story,
                    genai_client=genai_client,
                    single_prompt_template=single_prompt_template,
                    multi_prompt_template=multi_prompt_template,
                    source_article_cache=source_article_cache,
                )

            update_data = {
                "ref_articles": story["ref_articles"],
                "cover_images": get_cover_images(story["ref_articles"]),
                "updated_at": datetime.now(timezone.utc),  # UTC
                "latest_ref_article_at": get_latest_ref_article_at(
                    story["ref_articles"]
                ),
                "is_active": story.get("is_active", True),
                "is_visible": new_visibility,
                "headline": story.get("headline"),
                "summary": story.get("summary"),  # Include updated summary
            }

            bulk_updates.append(UpdateOne({"_id": story["_id"]}, {"$set": update_data}))

    if bulk_updates:
        db[HOT_STORIES_COLLECTION].bulk_write(bulk_updates)
        print(
            f"Updated {len(bulk_updates)} existing stories (Deactivated {deactivated_count})."
        )
        has_db_operation = True

    # 9. Mark source articles as aggregated (needs_aggregation=False)
    # Only execute after stories are successfully written to database
    if articles_to_mark:
        bulk_updates_by_collection = {coll_name: [] for coll_name in MONGO_COLLECTIONS}
        for coll_name, article_id in articles_to_mark:
            bulk_updates_by_collection[coll_name].append(
                UpdateOne({"_id": article_id}, {"$set": {"needs_aggregation": False}})
            )

        total_marked = 0
        for coll_name, updates in bulk_updates_by_collection.items():
            if updates:
                db[coll_name].bulk_write(updates)
                total_marked += len(updates)

        print(
            f"Marked {total_marked} source articles as aggregated (needs_aggregation=False)."
        )
        has_db_operation = True

    if not has_db_operation:
        print(
            "No aggregation actions were performed. No stories were created/updated and no source articles were marked."
        )

    client.close()


if __name__ == "__main__":
    main()
