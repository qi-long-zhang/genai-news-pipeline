"""
Select the best exemplar article for each topic from each collection.

Two-stage approach:
1. Embedding retrieval: Use Gemini embeddings to find top-K candidates per topic.
2. LLM reranking: Send candidates to Gemini LLM to select the most relevant article,
   resolving cases where keyword overlap misleads pure embedding similarity.
"""

import os
import json
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MONGO_COLLECTIONS = os.getenv("MONGO_COLLECTIONS", "").split(",")
if "channel_news_asia" not in MONGO_COLLECTIONS:
    MONGO_COLLECTIONS.append("channel_news_asia")

TOP_K = 20  # Number of embedding candidates to pass to LLM reranking

TOPICS_FILE = os.path.join(os.path.dirname(__file__), "..", "selected_topics.json")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "topic_exemplars.json")


def load_topics():
    with open(TOPICS_FILE, "r") as f:
        return json.load(f)


def generate_topic_embeddings(genai_client, topics):
    """
    Generate rich semantic embeddings for each topic.
    We expand each topic into a descriptive prompt to produce a more
    discriminative embedding in the same space as article embeddings.
    """
    topic_descriptions = [
        f"News article about {topic.lower()}. "
        f"Headline and lead paragraph covering {topic.lower()}."
        for topic in topics
    ]

    result = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=topic_descriptions,
        config=types.EmbedContentConfig(task_type="SEMANTIC_SIMILARITY"),
    )
    return {topic: np.array(e.values) for topic, e in zip(topics, result.embeddings)}


def cosine_similarity(a, b):
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def fetch_articles_with_embeddings(db, collection_name):
    """Fetch articles that have both article content and embeddings."""
    collection = db[collection_name]
    cursor = collection.find(
        {
            "article": {"$exists": True},
            "embedding.vector": {"$exists": True},
        },
        projection={
            "_id": 1,
            "article.title": 1,
            "article.subtitle": 1,
            "article.summary": 1,
            "article.content": 1,
            "article.url": 1,
            "article.source": 1,
            "article.publish_date": 1,
            "embedding.vector": 1,
        },
    )
    return list(cursor)


def format_article_for_reranking(article_info):
    """Format article fields for LLM reranking prompt."""
    parts = []
    if article_info.get("title"):
        parts.append(f"Title: {article_info['title']}")
    if article_info.get("subtitle"):
        parts.append(f"Subtitle: {article_info['subtitle']}")
    if article_info.get("summary"):
        parts.append(f"Summary: {article_info['summary']}")
    content = article_info.get("content") or []
    content_texts = []
    for block in content[:3]:
        text = block.get("text") if isinstance(block, dict) else str(block)
        if text:
            content_texts.append(text)
    if content_texts:
        parts.append(f"Content: {' '.join(content_texts)}")
    return "\n".join(parts)


def llm_rerank(genai_client, topic, candidates):
    """
    Use Gemini LLM to select the single most relevant article for a topic
    from a list of candidates.
    Returns the 0-based index of the best candidate.
    """
    candidate_texts = []
    for i, (art, score) in enumerate(candidates):
        article_info = art.get("article", {})
        formatted = format_article_for_reranking(article_info)
        candidate_texts.append(f"[Article {i}] (embedding_score={score:.4f})\n{formatted}")

    articles_block = "\n\n---\n\n".join(candidate_texts)

    prompt = f"""You are a news editor. Your task is to select the ONE article that is most genuinely about the topic "{topic}".

Important: An article must be **primarily and substantively** about this topic, not merely mentioning a keyword in passing. For example, a layoff news article that mentions "sports coverage" is NOT a sports article.

Here are the candidate articles:

{articles_block}

Respond with ONLY the article number (e.g., "3") of the single best match. If none are relevant, respond with the number of the least irrelevant one."""

    response = genai_client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt,
    )

    # Parse the response to get the index
    text = response.text.strip()
    # Extract first number from response
    for token in text.split():
        cleaned = token.strip("[]().,:;")
        if cleaned.isdigit():
            idx = int(cleaned)
            if 0 <= idx < len(candidates):
                return idx
    return 0  # fallback to embedding top-1


def select_exemplars(db, genai_client, topics, topic_embeddings):
    """
    Two-stage selection:
    1. Embedding similarity to get top-K candidates per topic.
    2. LLM reranking to pick the best from those candidates.
    """
    results = {}

    for coll_name in MONGO_COLLECTIONS:
        print(f"\n{'='*60}")
        print(f"Processing collection: {coll_name}")
        print(f"{'='*60}")

        articles = fetch_articles_with_embeddings(db, coll_name)
        print(f"  Found {len(articles)} articles with embeddings")

        if not articles:
            results[coll_name] = {topic: None for topic in topics}
            continue

        # Pre-compute article embedding arrays
        article_vectors = []
        valid_articles = []
        for art in articles:
            vec = art.get("embedding", {}).get("vector")
            if vec and len(vec) > 0:
                article_vectors.append(np.array(vec))
                valid_articles.append(art)

        if not valid_articles:
            results[coll_name] = {topic: None for topic in topics}
            continue

        # Stack into matrix for vectorized similarity computation
        article_matrix = np.stack(article_vectors)  # (N, 768)
        norms = np.linalg.norm(article_matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1
        article_matrix_normed = article_matrix / norms

        coll_results = {}
        for topic in topics:
            topic_vec = topic_embeddings[topic]
            topic_norm = np.linalg.norm(topic_vec)
            if topic_norm == 0:
                coll_results[topic] = None
                continue
            topic_vec_normed = topic_vec / topic_norm

            # Stage 1: Top-K by embedding similarity
            similarities = article_matrix_normed @ topic_vec_normed
            top_k_indices = np.argsort(similarities)[-TOP_K:][::-1]
            candidates = [
                (valid_articles[idx], float(similarities[idx]))
                for idx in top_k_indices
            ]

            # Stage 2: LLM reranking
            best_candidate_idx = llm_rerank(genai_client, topic, candidates)
            best_article, best_emb_score = candidates[best_candidate_idx]

            article_info = best_article.get("article", {})
            coll_results[topic] = {
                "title": article_info.get("title"),
                "subtitle": article_info.get("subtitle", ""),
                "url": article_info.get("url"),
                "source": article_info.get("source"),
                "publish_date": str(article_info.get("publish_date", "")),
                "similarity_score": round(best_emb_score, 4),
                "rerank_position": best_candidate_idx,
            }
            reranked = f" (reranked from #{best_candidate_idx})" if best_candidate_idx > 0 else ""
            print(
                f"  [{topic}] score={best_emb_score:.4f}{reranked} | "
                f"{article_info.get('title', 'N/A')[:70]}"
            )

        results[coll_name] = coll_results

    return results


def main():
    topics = load_topics()
    print(f"Loaded {len(topics)} topics from {TOPICS_FILE}")

    # Initialize clients
    genai_client = genai.Client(api_key=GEMINI_API_KEY)
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DATABASE]

    try:
        # Step 1: Generate topic embeddings
        print("\nGenerating topic embeddings via Gemini...")
        topic_embeddings = generate_topic_embeddings(genai_client, topics)
        print(f"Generated embeddings for {len(topic_embeddings)} topics")

        # Step 2: Select best exemplar per (collection, topic) with LLM reranking
        results = select_exemplars(db, genai_client, topics, topic_embeddings)

        # Step 3: Save results
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {OUTPUT_FILE}")

        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        for coll_name in MONGO_COLLECTIONS:
            coll_data = results.get(coll_name, {})
            matched = sum(1 for v in coll_data.values() if v is not None)
            print(f"  {coll_name}: {matched}/{len(topics)} topics matched")

    finally:
        mongo_client.close()


if __name__ == "__main__":
    main()
