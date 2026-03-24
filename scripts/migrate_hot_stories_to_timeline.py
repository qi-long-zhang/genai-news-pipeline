import argparse
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
HOT_STORIES_COLLECTION = "hot_stories"
BULK_WRITE_BATCH_SIZE = 500


def build_timeline_source_refs(ref_articles):
    """
    Build lightweight source references for a timeline entry.
    """
    return [
        {
            "article_id": ref.get("article_id"),
            "collection": ref.get("collection"),
            "url": ref.get("url"),
            "title": ref.get("title"),
            "update_date": ref.get("update_date"),
        }
        for ref in (ref_articles or [])
    ]


def build_timeline_entry(
    summary_text, ref_articles, created_at=None, entry_type="initial"
):
    """
    Build a normalized timeline entry.
    """
    return {
        "type": entry_type,
        "created_at": created_at or datetime.now(timezone.utc),
        "summary": (summary_text or "").strip(),
        "source_refs": build_timeline_source_refs(ref_articles),
    }


def format_timeline_timestamp(value):
    """
    Render timestamps consistently for the derived summary field.
    """
    if not isinstance(value, datetime):
        return ""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def render_timeline_summary(timeline):
    """
    Render timeline entries into the cached summary string.
    """
    rendered_entries = []
    for entry in timeline or []:
        summary_text = (entry.get("summary") or "").strip()
        if not summary_text:
            continue
        timestamp = format_timeline_timestamp(entry.get("created_at"))
        rendered_entries.append(
            f"{timestamp}: {summary_text}" if timestamp else summary_text
        )
    return "\n\n".join(rendered_entries)


def normalize_timeline_entry(entry, story, index):
    """
    Normalize a single existing timeline entry.
    """
    if not isinstance(entry, dict):
        entry = {"summary": str(entry), "type": "update" if index else "initial"}

    created_at = (
        entry.get("created_at") or story.get("updated_at") or story.get("created_at")
    )
    entry_type = entry.get("type") or ("initial" if index == 0 else "update")
    source_refs = entry.get("source_refs")
    if not isinstance(source_refs, list):
        source_refs = build_timeline_source_refs(story.get("ref_articles"))

    return {
        "type": entry_type,
        "created_at": created_at,
        "summary": (entry.get("summary") or "").strip(),
        "source_refs": source_refs,
    }


def build_normalized_timeline(story):
    """
    Build the canonical timeline for a story.
    """
    timeline = story.get("timeline")
    if isinstance(timeline, list) and timeline:
        normalized = [
            normalize_timeline_entry(entry, story, index)
            for index, entry in enumerate(timeline)
        ]
        normalized = [entry for entry in normalized if entry.get("summary")]
        if normalized:
            return normalized

    existing_summary = (story.get("summary") or "").strip()
    if not existing_summary:
        return []

    created_at = story.get("updated_at") or story.get("created_at")
    return [
        build_timeline_entry(
            summary_text=existing_summary,
            ref_articles=story.get("ref_articles"),
            created_at=created_at,
            entry_type="initial",
        )
    ]


def build_update_operation(story):
    """
    Create an UpdateOne operation when the story needs migration.
    """
    normalized_timeline = build_normalized_timeline(story)
    rendered_summary = render_timeline_summary(normalized_timeline)

    if (
        story.get("timeline") == normalized_timeline
        and (story.get("summary") or "") == rendered_summary
    ):
        return None

    return UpdateOne(
        {"_id": story["_id"]},
        {
            "$set": {
                "timeline": normalized_timeline,
                "summary": rendered_summary,
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
        description="Migrate hot_stories documents to timeline format."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and report how many stories would be updated without writing changes.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not MONGO_URI:
        raise SystemExit("MONGO_URI is required.")
    if not MONGO_DATABASE:
        raise SystemExit("MONGO_DATABASE is required.")

    client = MongoClient(MONGO_URI, tz_aware=True)
    collection = client[MONGO_DATABASE][HOT_STORIES_COLLECTION]

    scanned_count = 0
    updated_count = 0
    bulk_updates = []

    try:
        cursor = collection.find({})
        for story in cursor:
            scanned_count += 1
            update_op = build_update_operation(story)
            if not update_op:
                continue

            bulk_updates.append(update_op)
            if len(bulk_updates) >= BULK_WRITE_BATCH_SIZE:
                updated_count += flush_bulk_updates(
                    collection=collection,
                    bulk_updates=bulk_updates,
                    dry_run=args.dry_run,
                )
                bulk_updates = []

        updated_count += flush_bulk_updates(
            collection=collection,
            bulk_updates=bulk_updates,
            dry_run=args.dry_run,
        )
    finally:
        client.close()

    mode = "Would update" if args.dry_run else "Updated"
    print(f"Scanned {scanned_count} stories.")
    print(f"{mode} {updated_count} stories.")


if __name__ == "__main__":
    main()
