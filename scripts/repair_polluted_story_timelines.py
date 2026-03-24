import argparse
import os
import re
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne

HOT_STORIES_COLLECTION = "hot_stories"
TIMELINE_NO_UPDATE = "NO_UPDATE"
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


def is_polluted_timeline_summary(summary_text):
    """
    Determine whether a timeline entry summary is the NO_UPDATE sentinel.
    """
    return normalize_marker_text(summary_text) == normalize_marker_text(
        TIMELINE_NO_UPDATE
    )


def format_timeline_timestamp(value):
    """
    Format timestamps for rendered summary text.
    """
    if not isinstance(value, datetime):
        return ""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def get_timeline_sort_key(entry):
    """
    Build a stable sort key for timeline entries.
    """
    if not isinstance(entry, dict):
        return datetime.min.replace(tzinfo=timezone.utc)

    event_at = entry.get("event_at")
    if isinstance(event_at, datetime):
        return event_at

    created_at = entry.get("created_at")
    if isinstance(created_at, datetime):
        return created_at

    return datetime.min.replace(tzinfo=timezone.utc)


def sort_timeline_entries(timeline):
    """
    Sort timeline entries in descending display order in place.
    """
    timeline.sort(key=get_timeline_sort_key, reverse=True)
    return timeline


def render_timeline_summary(timeline):
    """
    Render timeline entries into the cached summary string.
    """
    rendered_entries = []
    for entry in sort_timeline_entries(list(timeline or [])):
        if not isinstance(entry, dict):
            continue

        summary_text = (entry.get("summary") or "").strip()
        if not summary_text:
            continue

        timestamp = format_timeline_timestamp(
            entry.get("event_at") or entry.get("created_at")
        )
        rendered_entries.append(
            f"{timestamp}: {summary_text}" if timestamp else summary_text
        )
    return "\n\n".join(rendered_entries)


def build_update_operation(story):
    """
    Remove polluted timeline entries from a story and refresh the summary.
    """
    timeline = story.get("timeline") or []
    cleaned_timeline = []
    removed_entries = []

    for entry in timeline:
        if not isinstance(entry, dict):
            cleaned_timeline.append(entry)
            continue

        summary_text = (entry.get("summary") or "").strip()
        if is_polluted_timeline_summary(summary_text):
            removed_entries.append(entry)
            continue

        cleaned_timeline.append(entry)

    if len(removed_entries) == 0:
        return None, 0

    sort_timeline_entries(cleaned_timeline)
    rendered_summary = render_timeline_summary(cleaned_timeline)

    return (
        UpdateOne(
            {"_id": story["_id"]},
            {
                "$set": {
                    "timeline": cleaned_timeline,
                    "summary": rendered_summary,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        ),
        len(removed_entries),
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
        description="Repair hot_stories timelines polluted with NO_UPDATE entries."
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

    if not mongo_uri:
        raise SystemExit("MONGO_URI is required.")
    if not mongo_database:
        raise SystemExit("MONGO_DATABASE is required.")

    client = MongoClient(mongo_uri, tz_aware=True)
    collection = client[mongo_database][HOT_STORIES_COLLECTION]

    scanned_count = 0
    polluted_story_count = 0
    polluted_entry_count = 0
    unresolved_count = 0
    bulk_updates = []
    planned_changes = []

    try:
        cursor = collection.find(
            {},
            {
                "headline": 1,
                "timeline": 1,
                "summary": 1,
                "updated_at": 1,
            },
        )
        for story in cursor:
            scanned_count += 1
            update_op, removed_count = build_update_operation(story)
            if not update_op:
                continue

            polluted_story_count += 1
            polluted_entry_count += removed_count
            if removed_count == 0:
                unresolved_count += 1
                continue

            planned_changes.append(
                {
                    "_id": str(story.get("_id")),
                    "headline": story.get("headline") or "",
                    "removed_count": removed_count,
                    "timeline_count": len(story.get("timeline") or []),
                }
            )
            bulk_updates.append(update_op)
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
        raise SystemExit(f"Repair failed: {exc}") from exc

    for change in planned_changes:
        action = "Updated" if args.apply else "Would update"
        print(
            f"{action} {change['_id']} [removed={change['removed_count']}, timeline={change['timeline_count']}]: "
            f"{change['headline']}"
        )

    if args.apply:
        print(
            f"Scanned {scanned_count} stories. Found {polluted_story_count} polluted stories. "
            f"Removed {polluted_entry_count} polluted timeline entries."
        )
    else:
        print(
            f"Scanned {scanned_count} stories. Found {polluted_story_count} polluted stories. "
            f"Would remove {polluted_entry_count} polluted timeline entries."
        )

    if unresolved_count:
        print(
            f"Skipped {unresolved_count} polluted stories with no removable timeline entries."
        )


if __name__ == "__main__":
    main()
