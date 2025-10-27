import requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
TARGET_ACCOUNT = os.getenv("TARGET_ACCOUNT")  # The account you want to monitor
MONGO_URI = os.getenv("MONGO_URI")  # MongoDB Atlas connection string
MONGO_DATABASE = os.getenv("MONGO_DATABASE")  # Database name
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")  # Collection name


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
    # Send HEAD request to follow redirects without downloading content
    response = requests.head(short_url, allow_redirects=True, timeout=5)
    return response.url


def extract_tweet_fields(tweet):
    """Extract only the fields we want to store in the database"""
    # Safely extract cover image (first media item if exists)
    media_list = tweet.get("extendedEntities", {}).get("media", [])
    cover_image = media_list[0].get("media_url_https") if media_list else None

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
    }


def update_tweets():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    now = datetime.now(timezone.utc)
    tweet_cursor = collection.find(
        {"needs_update": True, "created_at": {"$lte": now - timedelta(days=5)}},
        projection={"_id": 1, "created_at": 1},
    )

    tweets_to_update = [tweet["_id"] for tweet in tweet_cursor]

    if not tweets_to_update:
        print("No tweets need updating.")
        client.close()
        return

    print(f"Found {len(tweets_to_update)} tweets to update engagement metrics.")

    # Construct the query
    query = f"tweets_ids:{tweets_to_update}"

    # API endpoint
    url = "https://api.twitterapi.io/twitter/tweets"

    # Request parameters
    params = {"query": query, "queryType": "Latest"}

    # Headers with API key
    headers = {"X-API-Key": API_KEY}

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
                continue
            break
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break

    if all_tweets:
        # Update engagement data for each tweet
        updated_count = 0

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

            # Update the tweet in MongoDB
            result = collection.update_one(
                {"_id": tweet_id},
                {
                    "$set": {
                        "engagement": updated_engagement,
                        "needs_update": False,
                    }
                },
            )

            if result.modified_count > 0:
                updated_count += 1

        print(f"Successfully updated engagement metrics for {updated_count} tweets.")

    # Close the connection
    client.close()


def ingest_fresh_tweets():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # Retrieve the latest record and lasted created_at
    latest_record = collection.find_one(
        sort=[("created_at", -1)],
        projection={"created_at": 1},
    )
    latest_created_at = latest_record.get("created_at")

    # Format times for the API query
    since_time = latest_created_at + timedelta(seconds=1)
    until_time = datetime.now(timezone.utc)

    # Format times as strings in the format Twitter's API expects
    since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    # Construct the query
    query = f"from:{TARGET_ACCOUNT} since:{since_str} until:{until_str} include:nativeretweets"

    # API endpoint
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"

    # Request parameters
    params = {"query": query, "queryType": "Latest"}

    # Headers with API key
    headers = {"X-API-Key": API_KEY}

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
                continue
            break
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break

    if all_tweets:
        # Process all collected tweets
        print(f"Found {len(all_tweets)} total tweets from {TARGET_ACCOUNT}!")

        # Extract only the fields we need and add metadata
        processed_tweets = []

        for tweet in all_tweets:
            processed_tweet = extract_tweet_fields(tweet)
            processed_tweet["source_account"] = TARGET_ACCOUNT
            processed_tweet["needs_update"] = until_time - processed_tweet[
                "created_at"
            ] <= timedelta(days=5)
            processed_tweet["needs_scraping"] = True  # Mark for scraping

            processed_tweets.append(processed_tweet)

        # Insert all tweets at once
        result = collection.insert_many(processed_tweets, ordered=False)
        print(f"Successfully inserted {len(result.inserted_ids)} tweets into MongoDB!")
    else:
        print(f"No tweets found from {TARGET_ACCOUNT} in the specified time range.")

    # Close the connection
    client.close()


def main():
    print(f"Starting to update tweets from @{TARGET_ACCOUNT}")
    update_tweets()
    time.sleep(1)  # Small delay between operations
    print(f"Starting to ingest fresh tweets from @{TARGET_ACCOUNT}")
    ingest_fresh_tweets()


if __name__ == "__main__":
    main()
