import os
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "tweet2news")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "tweets")
TARGET_ACCOUNTS = os.getenv("TARGET_ACCOUNTS", "MothershipSG").split(",")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # 5 minutes
POPULARITY_THRESHOLD = int(os.getenv("POPULARITY_THRESHOLD", "10000"))

TWITTER_API_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"


def get_collection():
    """Connect to MongoDB and return collection"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]
    collection.create_index("tweetId", unique=True)
    return collection


def fetch_tweets(account, since_time, until_time):
    """Fetch tweets from Twitter API"""
    since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    query = f"from:{account} since:{since_str} until:{until_str} include:nativeretweets"

    headers = {"X-API-Key": API_KEY}
    params = {"query": query, "queryType": "Latest"}

    all_tweets = []
    next_cursor = None

    while True:
        if next_cursor:
            params["cursor"] = next_cursor

        response = requests.get(TWITTER_API_URL, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            print(f"  API error: {response.status_code}")
            break

        data = response.json()
        tweets = data.get("tweets", [])
        all_tweets.extend(tweets)

        if data.get("has_next_page") and data.get("next_cursor"):
            next_cursor = data["next_cursor"]
            params = {"query": query, "queryType": "Latest"}  # Reset params
        else:
            break

    return all_tweets


def parse_tweet_time(time_str):
    """Parse Twitter datetime string"""
    dt = datetime.strptime(time_str, "%a %b %d %H:%M:%S %z %Y")
    return dt.astimezone(timezone.utc)


def discover_new_tweets(collection):
    """Discover and save new tweets"""
    print(f"\n[{datetime.now(timezone.utc)}] Discovering new tweets...")
    now = datetime.now(timezone.utc)

    for account in TARGET_ACCOUNTS:
        account = account.strip()

        # Get last tweet time from database
        last_tweet = collection.find_one(
            {"account": account},
            sort=[("createdAt", -1)]
        )

        if last_tweet:
            since_time = last_tweet["createdAt"] - timedelta(minutes=1)
        else:
            since_time = now - timedelta(hours=1)

        # Fetch tweets
        print(f"@{account}: Fetching tweets since {since_time}")
        tweets = fetch_tweets(account, since_time, now)

        if not tweets:
            print(f"@{account}: No new tweets")
            continue

        print(f"@{account}: Found {len(tweets)} tweets")

        # Process each tweet
        for tweet in tweets:
            created_at = parse_tweet_time(tweet["createdAt"])
            age_hours = (now - created_at).total_seconds() / 3600

            # Determine when to collect metrics
            if age_hours >= 72:
                # Already mature, collect metrics now
                collect_at = now
                can_predict = False
                mode = "historical"
            elif age_hours >= 1:
                # Wait until 72h
                collect_at = created_at + timedelta(hours=72)
                can_predict = False
                mode = "delayed"
            else:
                # Fresh tweet, collect at 72h, can predict now
                collect_at = created_at + timedelta(hours=72)
                can_predict = True
                mode = "realtime"

            # Build document
            doc = {
                "tweetId": tweet["id"],
                "account": account,
                "text": tweet.get("text"),
                "url": tweet.get("url"),
                "createdAt": created_at,
                "discoveredAt": now,
                "collectMetricsAt": collect_at,
                "canPredict": can_predict,
                "mode": mode,
                "metricsCollected": False,
                "predictionMade": False,
            }

            # Insert if not exists
            collection.update_one(
                {"tweetId": doc["tweetId"]},
                {"$setOnInsert": doc},
                upsert=True
            )

            print(f"  → Tweet {tweet['id'][:8]}... ({mode}, age={age_hours:.1f}h)")


def collect_metrics(collection):
    """Collect metrics for tweets that are ready"""
    print(f"\n[{datetime.now(timezone.utc)}] Collecting metrics...")
    now = datetime.now(timezone.utc)

    # Find tweets ready for metrics collection
    tweets = collection.find({
        "collectMetricsAt": {"$lte": now},
        "metricsCollected": False
    })

    tweets = list(tweets)
    if not tweets:
        print("No tweets ready for metrics collection")
        return

    print(f"Found {len(tweets)} tweets to update")

    for tweet_doc in tweets:
        account = tweet_doc["account"]
        created_at = tweet_doc["createdAt"]
        tweet_id = tweet_doc["tweetId"]

        # Fetch tweet from API
        start = created_at - timedelta(minutes=1)
        end = created_at + timedelta(minutes=1)
        api_tweets = fetch_tweets(account, start, end)

        # Find matching tweet
        matching = next((t for t in api_tweets if t["id"] == tweet_id), None)

        if not matching:
            print(f"  Tweet {tweet_id[:8]}... not found, skipping")
            collection.update_one(
                {"tweetId": tweet_id},
                {"$set": {"metricsCollected": "failed"}}
            )
            continue

        # Extract metrics
        view_count = matching.get("viewCount", 0)
        is_popular = view_count >= POPULARITY_THRESHOLD

        # Update document
        collection.update_one(
            {"tweetId": tweet_id},
            {"$set": {
                "viewCount": view_count,
                "likeCount": matching.get("likeCount"),
                "retweetCount": matching.get("retweetCount"),
                "replyCount": matching.get("replyCount"),
                "metricsCollected": True,
                "metricsCollectedAt": now,
                "isPopular": is_popular,
            }}
        )

        label = "POPULAR" if is_popular else "not popular"
        print(f"  ✓ Tweet {tweet_id[:8]}... views={view_count} → {label}")


def main():
    """Main loop"""
    if not API_KEY or not MONGO_URI:
        print("Error: API_KEY and MONGO_URI must be set in .env")
        return

    collection = get_collection()

    accounts = ", ".join(f"@{a.strip()}" for a in TARGET_ACCOUNTS)
    print(f"Monitoring: {accounts}")
    print(f"Check interval: {CHECK_INTERVAL}s")
    print(f"Popularity threshold: {POPULARITY_THRESHOLD} views")
    print("=" * 60)

    try:
        while True:
            # Step 1: Discover new tweets
            discover_new_tweets(collection)

            # Step 2: Collect metrics for ready tweets
            collect_metrics(collection)

            # TODO: Step 3: Make predictions for fresh tweets (canPredict=True)

            print(f"\nSleeping {CHECK_INTERVAL}s...\n" + "=" * 60)
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
