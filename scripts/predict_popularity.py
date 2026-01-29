import os
import time
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from gradio_client import Client

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")
if "channel_news_asia" not in MONGO_COLLECTIONS:
    MONGO_COLLECTIONS.append("channel_news_asia")
BATCH_SIZE = int(
    os.getenv("PREDICTION_BATCH_SIZE", 10)
)  # Number of articles to process in each batch


def format_article_for_prediction(article):
    """Format article data into the expected input format for the model."""
    headline = article.get("title") or ""
    subhead = article.get("subtitle") or ""
    content = article.get("content") or []
    lead = ""
    if content:
        text_0 = content[0].get("text") or ""
        text_0_lower = text_0.lower()

        if (
            "update on" not in text_0_lower
            and "editor's note" not in text_0_lower
            and "warning:" not in text_0_lower
        ):
            lead = text_0
        elif len(content) > 1:
            lead = content[1].get("text") or ""

    return f"Headline: {headline}\nSubhead: {subhead}\nLead: {lead}"


def predict_batch(client, articles_data):
    # Prepare texts for batch prediction
    texts = [data[1] for data in articles_data]
    predictions = []

    try:
        # Call the Gradio client with the list of texts
        # Expecting the API to handle a list and return a list of results
        results = client.predict(texts, api_name="/predict")

        for i, (doc_id, formatted_text) in enumerate(articles_data):
            prediction = results[i]
            predictions.append((doc_id, prediction, formatted_text))

    except Exception as e:
        print(f"Error during batch prediction: {e}")
        return []

    return predictions


def process_collection(mongo_collection):
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[mongo_collection]

    # Find documents that need prediction and have article data
    cursor = collection.find(
        {
            "needs_popularity_prediction": True,
            "article": {"$exists": True},
        },
        projection={"_id": 1, "article": 1},
    )

    documents = list(cursor)

    if not documents:
        print(f"No documents need popularity prediction in {mongo_collection}.")
        client.close()
        return

    print(f"Found {len(documents)} documents needing prediction.")

    # Initialize Gradio Client
    try:
        classifier = Client("zhang-qilong/ModernBERT-News")
    except Exception as e:
        print(f"Failed to connect to Gradio Client: {e}")
        client.close()
        return

    # Process in batches
    total_processed = 0
    total_failed = 0

    for i in range(0, len(documents), BATCH_SIZE):
        batch = documents[i : i + BATCH_SIZE]

        # Prepare batch data
        batch_data = []
        for doc in batch:
            doc_id = doc["_id"]
            article = doc.get("article", {})

            # Skip if article is missing required fields
            if not article.get("title"):
                print(f"Warning: Document {doc_id} missing title, skipping.")
                total_failed += 1
                continue

            formatted_text = format_article_for_prediction(article)
            batch_data.append((doc_id, formatted_text))

        if not batch_data:
            continue

        # Make predictions
        predictions = predict_batch(classifier, batch_data)

        if not predictions:
            # If all failed in the batch
            total_failed += len(batch_data)
            continue

        # Calculate failures in batch (batch_size - successful predictions)
        total_failed += len(batch_data) - len(predictions)

        # Update database with bulk write
        operations = []
        for doc_id, prediction, formatted_text in predictions:
            op = UpdateOne(
                {"_id": doc_id},
                {
                    "$set": {
                        "popularity_prediction": {
                            "label": prediction.get("label"),
                            "score": prediction.get("score"),
                            "input_text": formatted_text,
                            "predicted_at": datetime.now(timezone.utc),
                        },
                        "needs_popularity_prediction": False,
                    }
                },
            )
            operations.append(op)

        if operations:
            result = collection.bulk_write(operations)
            total_processed += result.modified_count

    print(f"Successfully processed: {total_processed}")
    print(f"Failed: {total_failed}")
    print(f"Total: {len(documents)}")

    # Close MongoDB connection
    client.close()


def main():
    """Main function to process all collections."""
    for mongo_collection in MONGO_COLLECTIONS:
        print(f"Starting popularity prediction process for {mongo_collection}")
        process_collection(mongo_collection)
        print(f"--- Finished Collection: {mongo_collection} ---\n")
        time.sleep(1)  # Small delay between collections


if __name__ == "__main__":
    main()
