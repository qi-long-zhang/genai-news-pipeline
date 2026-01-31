import requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
import os
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

# Load environment variables from .env file
load_dotenv()

# Configuration
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TARGET_ACCOUNTS = os.getenv("TARGET_ACCOUNTS", "").split(",")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")

retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD"],
)
adapter = HTTPAdapter(max_retries=retries)
session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)


def parse_twitter_time(time_str):
    """Convert Twitter's time format to datetime object
    Example: 'Fri Oct 24 01:07:19 +0000 2025' -> datetime object
    """
    if not time_str:
        return None
    # Twitter format: "Fri Oct 24 01:07:19 +0000 2025"
    return datetime.strptime(time_str, "%a %b %d %H:%M:%S %z %Y")


def expand_url(short_url):
    """Expand shortened URL (like bit.ly) to get the real URL
    Example: 'http://bit.ly/47sMz3t' -> 'https://mothership.sg/...'
    """
    if not short_url:
        return None
    try:
        response = session.head(
            short_url,
            allow_redirects=True,
            timeout=(5, 10),  # connect timeout, read timeout
        )
        return response.url
    except requests.RequestException as e:
        print(f"Warning: Failed to expand URL {short_url}: {e}")
        return None


def extract_tweet_fields(tweet, target_account=None):
    """Extract only the fields we want to store in the database"""
    # Safely extract cover image (first media item if exists)
    media_list = tweet.get("extendedEntities", {}).get("media", [])
    cover_image = media_list[0].get("media_url_https") if media_list else None

    # Specific handling for straits_times cover image
    if target_account == "straits_times" and not cover_image:
        binding_values = tweet.get("card", {}).get("binding_values", [])
        for item in binding_values:
            if item.get("key") == "summary_photo_image_original":
                cover_image = item.get("value", {}).get("image_value", {}).get("url")
                break

    # Safely extract article URL (first URL if exists) and expand it
    urls_list = tweet.get("entities", {}).get("urls", [])
    short_url = urls_list[0].get("expanded_url") if urls_list else None
    article_url = expand_url(short_url) if short_url else None

    return {
        # Use tweet ID as MongoDB _id to prevent duplicates
        "_id": tweet.get("id"),
        # Basic info
        "x_url": tweet.get("url"),
        "created_at": parse_twitter_time(tweet.get("createdAt")),
        # Engagement metrics
        "engagement": {
            "retweet_count": tweet.get("retweetCount", 0),
            "reply_count": tweet.get("replyCount", 0),
            "like_count": tweet.get("likeCount", 0),
            "quote_count": tweet.get("quoteCount", 0),
            "view_count": tweet.get("viewCount", 0),
        },
        # Media content
        "cover_image": cover_image,
        # News article link
        "article_url": article_url,
        # News source
        "source": tweet.get("author", {}).get("name"),
    }


def chunked(sequence, size):
    for index in range(0, len(sequence), size):
        yield sequence[index : index + size]


def update_tweets(mongo_collection):
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[mongo_collection]

    now = datetime.now(timezone.utc)
    tweet_cursor = collection.find(
        {"needs_update": True, "created_at": {"$lte": now - timedelta(days=7)}},
        projection={"_id": 1, "created_at": 1},
    )

    tweets_to_update = [tweet["_id"] for tweet in tweet_cursor]

    if not tweets_to_update:
        print("No tweets need updating.")
        client.close()
        return

    print(f"Found {len(tweets_to_update)} tweets for engagement updates.")

    # API endpoint
    url = "https://api.twitterapi.io/twitter/tweets"

    # Headers with API key
    headers = {"X-API-Key": TWITTER_API_KEY}

    all_tweets = []

    def fetch_batch(id_chunk, label):
        nonlocal all_tweets
        response = requests.get(
            url,
            headers=headers,
            params={"tweet_ids": ",".join(id_chunk)},
        )

        if response.status_code == 200:
            data = response.json()
            tweets = data.get("tweets", [])
            all_tweets.extend(tweets)
        else:
            print(f"{label} Error: {response.status_code} - {response.text}")

    for chunk in chunked(tweets_to_update, 5):
        fetch_batch(chunk, f"Primary batch {chunk}")
        time.sleep(0.1)  # Small delay to avoid hitting rate limits

    returned_ids = {tweet.get("id") for tweet in all_tweets}
    missing_tweets = [tid for tid in tweets_to_update if tid not in returned_ids]

    retry_count = 0
    max_retries = 2
    while missing_tweets and retry_count < max_retries:
        retry_count += 1
        for chunk in chunked(missing_tweets, 5):
            fetch_batch(chunk, f"Retry batch {chunk}")
            time.sleep(0.1)  # Small delay to avoid hitting rate limits
        returned_ids = {tweet.get("id") for tweet in all_tweets}
        missing_tweets = [tid for tid in tweets_to_update if tid not in returned_ids]

    if missing_tweets:
        print(
            f"Stopped after {retry_count} retries; {len(missing_tweets)} tweets still missing: {missing_tweets}"
        )
    else:
        print(f"API returned {len(all_tweets)} tweets for engagement updates.")

    if all_tweets:
        # Update engagement data for each tweet
        operations = []

        for tweet in all_tweets:
            tweet_id = tweet.get("id")

            # Extract updated engagement metrics
            updated_engagement = {
                "retweet_count": tweet.get("retweetCount", 0),
                "reply_count": tweet.get("replyCount", 0),
                "like_count": tweet.get("likeCount", 0),
                "quote_count": tweet.get("quoteCount", 0),
                "view_count": tweet.get("viewCount", 0),
            }

            # Prepare bulk update operation
            operations.append(
                UpdateOne(
                    {"_id": tweet_id},
                    {
                        "$set": {
                            "engagement": updated_engagement,
                            "needs_update": False,
                        }
                    },
                )
            )

        if operations:
            result = collection.bulk_write(operations)
            print(
                f"Successfully updated engagement metrics for {result.modified_count} tweets."
            )

    # Close the connection
    client.close()


def ingest_fresh_tweets(target_account, mongo_collection):
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[mongo_collection]

    # Retrieve the latest record and lasted created_at
    latest_record = collection.find_one(
        sort=[("created_at", -1)],
        projection={"created_at": 1},
    )

    if not latest_record or not (latest_created_at := latest_record.get("created_at")):
        return

    # Normalize to UTC for consistent comparisons
    latest_created_at = latest_created_at.astimezone(timezone.utc)

    # Format times for the API query
    since_time = latest_created_at + timedelta(seconds=1)
    until_time = datetime.now(timezone.utc)

    # Format times as strings in the format Twitter's API expects
    since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    # Construct the query
    query = f"from:{target_account} since:{since_str} until:{until_str}"

    # API endpoint
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"

    # Request parameters
    params = {"query": query, "queryType": "Latest"}

    # Headers with API key
    headers = {"X-API-Key": TWITTER_API_KEY}

    # Make the request and handle pagination
    all_tweets = []
    next_cursor = None

    while True:
        # Add cursor to params if we have one
        if next_cursor:
            params["cursor"] = next_cursor

        response = requests.get(url, headers=headers, params=params)

        # Parse the response
        if response.status_code == 200:
            data = response.json()
            tweets = data.get("tweets", [])

            if tweets:
                all_tweets.extend(tweets)

            # Check if there are more pages
            next_cursor_value = data.get("next_cursor")
            if data.get("has_next_page", False) and next_cursor_value:
                next_cursor = next_cursor_value
                time.sleep(0.1)  # Add delay to avoid rate limits
                continue
            break
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break

    if all_tweets:
        # Filter out tweets not from the target account or outside the expected time range
        filtered_tweets = []
        skipped = 0

        for tweet in all_tweets:
            if (tweet.get("author") or {}).get("userName", "") != target_account:
                skipped += 1
                continue

            created_at = parse_twitter_time(tweet.get("createdAt"))
            if not created_at or created_at < since_time or created_at > until_time:
                skipped += 1
                continue

            filtered_tweets.append(tweet)

        if skipped:
            print(
                f"Skipped {skipped} tweets not matching @{target_account} or outside {since_time} to {until_time}."
            )

        if not filtered_tweets:
            print(
                "No tweets from the target account after filtering; nothing will be inserted."
            )
            client.close()
            return

        print(
            f"Found {len(filtered_tweets)} total tweets from {target_account} after filtering."
        )

        # Extract only the fields we need and add metadata
        processed_tweets = []

        def process_tweet(tweet):
            # Account specific pre-processing filtering for straits_times
            if target_account == "straits_times" and tweet.get("card") is None:
                return None

            processed_tweet = extract_tweet_fields(tweet, target_account)

            if processed_tweet.get("article_url") is None:
                return None

            # Account specific filtering
            if target_account == "straits_times":
                url = processed_tweet.get("article_url")
                if (
                    url == "https://www.straitstimes.com/"
                    or url == "https://www.straitstimes.com/global"
                ):
                    return None
                if (url or "").startswith("https://www.straitstimes.com/multimedia"):
                    return None
                if (url or "").startswith("https://www.straitstimes.com/newsletter"):
                    return None
                if processed_tweet.get("cover_image") is None:
                    return None

            processed_tweet["needs_update"] = until_time - processed_tweet[
                "created_at"
            ] <= timedelta(days=7)  # Mark for update if within 7 days
            processed_tweet["needs_scraping"] = True  # Mark for scraping

            return processed_tweet

        # Use ThreadPoolExecutor to process tweets in parallel (mainly for URL expansion)
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(process_tweet, filtered_tweets)

        for res in results:
            if res:
                processed_tweets.append(res)

        if not processed_tweets:
            print("No tweets remaining after account-specific filtering.")
            client.close()
            return

        # Deduplicate within the current batch based on article_url
        # Keep the first occurrence (which is the newest due to API order)
        seen_urls = set()
        unique_processed_tweets = []
        for tweet in processed_tweets:
            url = tweet.get("article_url")
            if url:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
            unique_processed_tweets.append(tweet)

        processed_tweets = unique_processed_tweets

        # Deduplicate based on article_url:
        # If a tweet in the new batch has an article_url that already exists in the DB,
        # delete the existing document(s) in the DB to allow the new one to take precedence.
        # This ensures we don't have multiple tweets pointing to the same article,
        # and we prefer the latest tweet (assuming fresh ingest is newer).
        article_urls = [
            t.get("article_url") for t in processed_tweets if t.get("article_url")
        ]
        if article_urls:
            delete_result = collection.delete_many(
                {"article_url": {"$in": article_urls}}
            )
            if delete_result.deleted_count > 0:
                print(
                    f"Deleted {delete_result.deleted_count} existing documents with duplicate article_urls."
                )

        # Insert all tweets at once
        result = collection.insert_many(processed_tweets, ordered=False)
        print(f"Successfully inserted {len(result.inserted_ids)} tweets into MongoDB!")
    else:
        print(f"No tweets found from {target_account} in the specified time range.")

    # Close the connection
    client.close()


def main():
    if len(TARGET_ACCOUNTS) != len(MONGO_COLLECTIONS):
        print(
            "Error: The number of TARGET_ACCOUNTS must match the number of MONGO_COLLECTIONS."
        )
        return

    for target_account, mongo_collection in zip(TARGET_ACCOUNTS, MONGO_COLLECTIONS):
        target_account = target_account.strip()
        mongo_collection = mongo_collection.strip()

        if not target_account or not mongo_collection:
            continue

        print(
            f"--- Processing Account: @{target_account} (Collection: {mongo_collection}) ---"
        )
        print(f"Starting to update tweets from @{target_account}")
        update_tweets(mongo_collection)
        time.sleep(0.1)  # Small delay between operations
        print(f"Starting to ingest fresh tweets from @{target_account}")
        ingest_fresh_tweets(target_account, mongo_collection)
        print(f"--- Finished Account: @{target_account} ---\n")


if __name__ == "__main__":
    main()
