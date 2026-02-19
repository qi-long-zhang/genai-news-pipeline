import os
import json
import logging
from typing import List, Dict, Any

from dotenv import load_dotenv
from datasets import load_dataset
from google import genai


# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DATASET_NAME = "zhang-qilong/SG-News-Summarization"
PROMPTS_FILE = "data/json/prompts.json"
OUTPUT_FILE = "data/json/generated_summaries.json"
# The user specifically requested this model.
MODEL_ID = "gemini-3-pro-preview"


def load_prompts(filepath: str) -> List[Dict[str, Any]]:
    """Loads prompts from a JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("prompts", [])
    except FileNotFoundError:
        logger.error(f"Prompts file not found at {filepath}")
        return []
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {filepath}")
        return []


def format_prompt(prompt_template: str, row: Dict[str, Any]) -> str:
    """Formats the prompt template with data from the dataset row."""
    # Map dataset columns to prompt placeholders
    # Dataset: title, subtitle, content, source, topic
    # Prompt placeholders: {headline}, {subhead}, {content}, {source}, {topic}

    format_kwargs = {
        "content": row.get("content", ""),
        "topic": row.get("topic", ""),
        "source": row.get("source", ""),
        "headline": row.get("title", ""),
        "subhead": row.get("subtitle", ""),
    }

    # We use safe formatting to ignore missing keys if the prompt doesn't strictly require them all
    # but the format method raises KeyError for missing keys.
    # We'll use a custom approach or just ensure all expected keys are present.
    # Since we provided all potential keys in format_kwargs, standard format() should work
    # assuming the prompt only uses these keys.

    try:
        return prompt_template.format(**format_kwargs)
    except KeyError as e:
        logger.warning(
            f"Missing key for prompt format: {e}. Available keys: {list(format_kwargs.keys())}"
        )
        # Fallback: try to return partially formatted or original if critical keys missing
        return prompt_template


def generate_summary(client: genai.Client, model_id: str, prompt: str) -> str:
    """Generates a summary using the Gemini API."""
    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
        )
        return response.text
    except Exception as e:
        logger.error(f"Error generating content: {e}")
        return f"[Error: {e}]"


def main():
    # Initialize Gemini Client
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error(
            "GOOGLE_API_KEY or GEMINI_API_KEY not found in environment variables."
        )
        return

    client = genai.Client(api_key=api_key)

    # Load Dataset
    logger.info(f"Loading dataset: {DATASET_NAME}")
    try:
        dataset = load_dataset(DATASET_NAME, split="train")
        # Optional: Limit for testing
        # dataset = dataset.select(range(5))
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        return

    # Load Prompts
    prompts_config = load_prompts(PROMPTS_FILE)
    if not prompts_config:
        logger.error("No prompts loaded. Exiting.")
        return

    results = []

    logger.info(
        f"Starting generation for {len(dataset)} articles with {len(prompts_config)} prompts each."
    )

    for i, row in enumerate(dataset):
        logger.info(
            f"Processing Article {i + 1}/{len(dataset)}: {row.get('title', 'Unknown Title')[:50]}..."
        )

        for prompt_cfg in prompts_config:
            technique = prompt_cfg.get("technique", "Unknown")
            prompt_template = prompt_cfg.get("prompt", "")

            if not prompt_template:
                continue

            formatted_prompt = format_prompt(prompt_template, row)

            logger.info(f"  - Generating with technique: {technique}")
            summary = generate_summary(client, MODEL_ID, formatted_prompt)

            results.append({
                "technique": technique,
                "input": row.get("content", ""),
                "actual_output": summary
            })

    # Save Results
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully saved results to {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Failed to save results: {e}")


if __name__ == "__main__":
    main()
