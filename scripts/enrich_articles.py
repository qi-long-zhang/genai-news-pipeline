import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from gradio_client import Client
from google import genai
from google.genai import types
from concurrent.futures import ThreadPoolExecutor

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")
if "channel_news_asia" not in MONGO_COLLECTIONS:
    MONGO_COLLECTIONS.append("channel_news_asia")
BATCH_SIZE = int(
    os.getenv("PREDICTION_BATCH_SIZE", 10)
)  # Number of articles to process in each batch

# Global MongoDB Client to be shared across threads
# PyMongo's MongoClient is thread-safe
global_mongo_client = None


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
            and "update at" not in text_0_lower
            and "editor" not in text_0_lower
            and "warning:" not in text_0_lower
        ):
            lead = text_0
        elif len(content) > 1:
            lead = content[1].get("text") or ""

    return f"Headline: {headline}\nSubhead: {subhead}\nLead: {lead}"


def get_embeddings_batch(client, texts):
    """Generate embeddings for a batch of texts using Gemini API."""
    try:
        result = client.models.embed_content(
            model="gemini-embedding-001",
            contents=texts,
            config=types.EmbedContentConfig(task_type="SEMANTIC_SIMILARITY"),
        )
        return [e.values for e in result.embeddings]
    except Exception as e:
        print(f"Error generating embeddings: {e}")


def predict_batch(client, texts):
    """Generate predictions for a batch of texts using Gradio API."""
    try:
        results = client.predict(texts, api_name="/predict")
        return results
    except Exception as e:
        print(f"Error during batch prediction: {e}")


def process_collection(mongo_collection):
    # Use the shared global client
    if global_mongo_client is None:
        raise RuntimeError("Global Mongo Client not initialized")
    db = global_mongo_client[MONGO_DATABASE]
    collection = db[mongo_collection]

    # Find documents that need prediction and have article data
    cursor = collection.find(
        {
            "needs_prediction": True,
            "article": {"$exists": True},
        },
        projection={"_id": 1, "article": 1},
    )

    documents = list(cursor)

    if not documents:
        print(f"[{mongo_collection}] No documents need enrichment.")
        return

    # Initialize Gradio Client
    try:
        classifier = Client("zhang-qilong/ModernBERT-News")
    except Exception as e:
        print(f"[{mongo_collection}] Failed to connect to Gradio Client: {e}")
        return

    # Initialize GenAI Client
    try:
        genai_client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception as e:
        print(f"[{mongo_collection}] Failed to init GenAI client: {e}")
        return

    # Process in batches
    total_processed = 0

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
                continue

            formatted_text = format_article_for_prediction(article)
            batch_data.append((doc_id, formatted_text))

        if not batch_data:
            continue

        # Prepare texts for both prediction and embedding
        texts = [data[1] for data in batch_data]

        # Run prediction and embedding sequentially
        predictions = predict_batch(classifier, texts)
        embeddings = get_embeddings_batch(genai_client, texts)

        if not predictions or not embeddings:
            continue

        # Update database with bulk write
        operations = []
        for j, (doc_id, formatted_text) in enumerate(batch_data):
            prediction = predictions[j]
            update_doc = {
                "prediction": {
                    "label": prediction.get("label"),
                    "score": prediction.get("score"),
                    "predicted_at": datetime.now(timezone.utc),
                },
                "embedding": {
                    "text": formatted_text,
                    "vector": embeddings[j],
                },
                "needs_prediction": False,
            }

            op = UpdateOne(
                {"_id": doc_id},
                {"$set": update_doc},
            )
            operations.append(op)

        if operations:
            result = collection.bulk_write(operations)
            total_processed += result.modified_count

    print(
        f"[{mongo_collection}] Finished. Found: {len(documents)}, Processed: {total_processed}"
    )


def main():
    """Main function to process all collections concurrently."""
    global global_mongo_client
    # Initialize shared MongoDB client
    global_mongo_client = MongoClient(MONGO_URI)

    try:
        # Use ThreadPoolExecutor for collection-level concurrency
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(process_collection, MONGO_COLLECTIONS)
    finally:
        if global_mongo_client:
            global_mongo_client.close()


if __name__ == "__main__":
    main()
