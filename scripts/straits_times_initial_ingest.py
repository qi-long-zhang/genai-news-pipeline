import requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# Configuration
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TARGET_ACCOUNT = "straits_times"  # The account you want to monitor
MONGO_URI = os.getenv("MONGO_URI")  # MongoDB Atlas connection string
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")  # Database name
MONGO_COLLECTION = "straits_times"  # Collection name


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
    cover_image = None
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


def save_initial_tweets():
    # Format times for the API query
    # Get tweets from 14 days ago to 5 days ago
    until_time = datetime.now(timezone.utc) - timedelta(days=7)
    since_time = datetime.now(timezone.utc) - timedelta(days=8)

    # Format times as strings in the format Twitter's API expects
    since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    # Construct the query
    query = f"from:{TARGET_ACCOUNT} since:{since_str} until:{until_str}"
    # Please refer to this document for detailed Twitter advanced search syntax. https://github.com/igorbrigadir/twitter-advanced-search

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
        time.sleep(0.5)  # Wait 0.5 seconds between requests for free-tier rate limit

        # Parse the response
        if response.status_code == 200:
            data = response.json()
            tweets = data.get("tweets", [])

            if tweets:
                all_tweets.extend(tweets)

            # Check if there are more pages
            if data.get("has_next_page", False) and data.get("next_cursor", "") != "":
                next_cursor = data.get("next_cursor")
                continue
            else:
                break
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break

    # Process all collected tweets
    if all_tweets:
        print(f"Found {len(all_tweets)} total tweets from {TARGET_ACCOUNT}!")

        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]

        # Extract only the fields we need and add metadata
        processed_tweets = []

        for tweet in all_tweets:
            # Filter out tweets without card or cover image
            if tweet.get("card") is not None:
                processed_tweet = extract_tweet_fields(tweet)

                if processed_tweet.get("cover_image") is not None:
                    # Filter out tweets where article_url is just the homepage
                    if (
                        processed_tweet.get("article_url")
                        == "https://www.straitstimes.com/"
                    ):
                        continue

                    processed_tweet["needs_update"] = False
                    processed_tweet["needs_scraping"] = True  # Mark for scraping

                    processed_tweets.append(processed_tweet)

        # Insert all tweets at once
        result = collection.insert_many(processed_tweets)
        print(f"Successfully inserted {len(result.inserted_ids)} tweets into MongoDB!")

        # Close the connection
        client.close()
    else:
        print(f"No tweets found from {TARGET_ACCOUNT} in the specified time range.")


# Main monitoring loop
def main():
    print(f"Starting to save initial tweets from @{TARGET_ACCOUNT}")
    save_initial_tweets()


if __name__ == "__main__":
    main()
