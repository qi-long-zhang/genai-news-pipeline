import argparse
import os
import re
from datetime import datetime, timezone

from google import genai
from pymongo import MongoClient, UpdateOne

HOT_STORIES_COLLECTION = "hot_stories"
HEADLINE_NO_UPDATE = "NO_HEADLINE_UPDATE"
MODEL_ID = "gemini-3.1-pro-preview"
BULK_WRITE_BATCH_SIZE = 500


def load_env_file(path=".env"):
    """
    Load key-value pairs from a local .env file when process env is unset.
    """
    values = {}
    if not os.path.exists(path):
        return values

    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")

    return values


def get_config_value(name, env_file_values):
    """
    Read a config value from the environment first, then the local .env file.
    """
    value = os.getenv(name)
    if value:
        return value
    return env_file_values.get(name, "")


def normalize_marker_text(text):
    """
    Normalize sentinel text for comparisons.
    """
    return re.sub(r"[\s\.\!\-_:]+", "", (text or "")).upper()


def is_polluted_headline(headline):
    """
    Determine whether the stored headline is a sentinel value.
    """
    return normalize_marker_text(headline) == normalize_marker_text(HEADLINE_NO_UPDATE)


def format_timeline_timestamp(value):
    """
    Format timestamps for fallback prompt context.
    """
    if not isinstance(value, datetime):
        return ""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def render_story_summary_text(story):
    """
    Use the cached summary when available, otherwise derive one from timeline.
    """
    summary = (story.get("summary") or "").strip()
    if summary:
        return summary

    rendered_entries = []
    for entry in story.get("timeline") or []:
        summary_text = (entry.get("summary") or "").strip()
        if not summary_text:
            continue

        timestamp = format_timeline_timestamp(
            entry.get("event_at") or entry.get("created_at")
        )
        rendered_entries.append(
            f"{timestamp}: {summary_text}" if timestamp else summary_text
        )

    return "\n\n".join(rendered_entries).strip()


def get_primary_ref_title(story):
    """
    Resolve the best available title from existing story data.
    """
    ref_articles = story.get("ref_articles") or []
    if ref_articles:
        title = ref_articles[0].get("title")
        if title:
            return title.strip()

    for entry in story.get("timeline") or []:
        source_refs = entry.get("source_refs") or []
        for ref in source_refs:
            title = ref.get("title")
            if title:
                return title.strip()

    return ""


def build_mult_article_prompt(story):
    """
    Build an LLM prompt for a polluted multi-article story headline repair.
    """
    summary_text = render_story_summary_text(story) or "No story summary available."
    titles = [
        ref.get("title")
        for ref in (story.get("ref_articles") or [])
        if ref.get("title")
    ]
    titles_text = "\n".join(f"- {title}" for title in titles[:5]) or "- None"

    return f"""You are a news headline editor.

Write one concise factual headline in sentence case for this news story.
Use only the information provided below.
Return only the headline, with no label, explanation, or quotation marks.

Story summary:
{summary_text}

Existing article titles:
{titles_text}
"""


def extract_headline_text(response_text):
    """
    Extract the headline text from an LLM response.
    """
    text = (response_text or "").strip()
    if not text:
        return ""

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^(final\s+)?headline\s*:\s*", "", line, flags=re.IGNORECASE)
        line = line.strip().strip('"').strip("'").strip()
        if line:
            return line

    return text.splitlines()[0].strip().strip('"').strip("'")


def generate_llm_headline(story, genai_client):
    """
    Generate a replacement headline for a multi-article polluted story.
    """
    prompt = build_mult_article_prompt(story)
    response = genai_client.models.generate_content(
        model=MODEL_ID,
        contents=prompt,
    )
    return extract_headline_text(getattr(response, "text", ""))


def build_repair_target(story, genai_client=None):
    """
    Determine the replacement headline and its source.
    """
    ref_articles = story.get("ref_articles") or []
    if len(ref_articles) <= 1:
        headline = get_primary_ref_title(story)
        return headline, "ref_title"

    if genai_client is None:
        return "", "missing_api_key"

    headline = generate_llm_headline(story, genai_client)
    if headline and not is_polluted_headline(headline):
        return headline, "llm"

    fallback_headline = get_primary_ref_title(story)
    if fallback_headline:
        return fallback_headline, "fallback_ref_title"

    return "", "llm_failed"


def build_update_operation(story, new_headline):
    """
    Create the Mongo update for a repaired story.
    """
    return UpdateOne(
        {"_id": story["_id"]},
        {
            "$set": {
                "headline": new_headline,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )


def flush_bulk_updates(collection, bulk_updates, dry_run):
    """
    Execute a batch of pending updates.
    """
    if not bulk_updates:
        return 0

    if dry_run:
        return len(bulk_updates)

    collection.bulk_write(bulk_updates, ordered=False)
    return len(bulk_updates)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Repair hot_stories headlines polluted with NO_HEADLINE_UPDATE."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the repair to MongoDB. Without this flag the script only reports what would change.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    env_file_values = load_env_file()
    mongo_uri = get_config_value("MONGO_URI", env_file_values)
    mongo_database = get_config_value("MONGO_DATABASE", env_file_values)
    api_key = get_config_value("GOOGLE_API_KEY", env_file_values) or get_config_value(
        "GEMINI_API_KEY", env_file_values
    )

    if not mongo_uri:
        raise SystemExit("MONGO_URI is required.")
    if not mongo_database:
        raise SystemExit("MONGO_DATABASE is required.")

    client = MongoClient(mongo_uri, tz_aware=True)
    collection = client[mongo_database][HOT_STORIES_COLLECTION]

    scanned_count = 0
    polluted_stories = []

    try:
        cursor = collection.find(
            {},
            {
                "headline": 1,
                "ref_articles": 1,
                "timeline": 1,
                "updated_at": 1,
                "summary": 1,
            },
        )
        for story in cursor:
            scanned_count += 1
            if is_polluted_headline(story.get("headline")):
                polluted_stories.append(story)
    except Exception as exc:
        raise SystemExit(f"Repair failed while scanning stories: {exc}") from exc

    if not polluted_stories:
        print(f"Scanned {scanned_count} stories. Found 0 polluted stories.")
        return

    single_article_count = 0
    multi_article_count = 0
    for story in polluted_stories:
        ref_count = len(story.get("ref_articles") or [])
        if ref_count <= 1:
            single_article_count += 1
        else:
            multi_article_count += 1

    if multi_article_count and not api_key:
        raise SystemExit(
            "GEMINI_API_KEY or GOOGLE_API_KEY is required to repair multi-article headlines."
        )

    genai_client = genai.Client(api_key=api_key) if multi_article_count else None

    unresolved_count = 0
    bulk_updates = []
    planned_changes = []

    try:
        for story in polluted_stories:
            old_headline = (story.get("headline") or "").strip()
            new_headline, repair_source = build_repair_target(story, genai_client)
            if not new_headline:
                unresolved_count += 1
                continue

            planned_changes.append(
                {
                    "_id": str(story.get("_id")),
                    "old_headline": old_headline,
                    "new_headline": new_headline,
                    "repair_source": repair_source,
                    "ref_count": len(story.get("ref_articles") or []),
                }
            )
            bulk_updates.append(build_update_operation(story, new_headline))
            if len(bulk_updates) >= BULK_WRITE_BATCH_SIZE:
                flush_bulk_updates(
                    collection=collection,
                    bulk_updates=bulk_updates,
                    dry_run=not args.apply,
                )
                bulk_updates = []

        if bulk_updates:
            flush_bulk_updates(
                collection=collection,
                bulk_updates=bulk_updates,
                dry_run=not args.apply,
            )

    except Exception as exc:
        raise SystemExit(f"Repair failed while building updates: {exc}") from exc

    for change in planned_changes:
        action = "Updated" if args.apply else "Would update"
        print(
            f"{action} {change['_id']} [{change['repair_source']}, refs={change['ref_count']}]: "
            f"{change['old_headline']} -> {change['new_headline']}"
        )

    polluted_count = len(polluted_stories)
    resolved_count = len(planned_changes)
    if args.apply:
        print(
            f"Scanned {scanned_count} stories. Found {polluted_count} polluted stories. Updated {resolved_count} stories."
        )
    else:
        print(
            f"Scanned {scanned_count} stories. Found {polluted_count} polluted stories. Would update {resolved_count} stories."
        )

    if unresolved_count:
        print(f"Skipped {unresolved_count} polluted stories with no repair headline.")


if __name__ == "__main__":
    main()
