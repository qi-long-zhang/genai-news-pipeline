# genai-news-pipeline

`genai-news-pipeline` is the upstream content pipeline for `genai-news`.

It is responsible for:

- scraping articles from news sites
- enriching fresh articles with classification and embeddings
- aggregating related articles into `hot_stories`
- running the pipeline on a schedule with GitHub Actions

Current built-in sources:

- Mothership
- The Straits Times
- CNA

## Data Flow

The main pipeline is:

1. `scripts/run_spiders.py`
   Runs all Scrapy spiders, scrapes article content, and writes it back to MongoDB.
2. `scripts/enrich_articles.py`
   Classifies fresh articles and generates Gemini embeddings.
3. `scripts/aggregate_stories.py`
   Aggregates related articles into `hot_stories` using embeddings, article recency, and prompts.

There is also an optional script:

- `scripts/sync_tweets.py`
  Syncs X post metadata from `twitterapi.io` into MongoDB for configured accounts.

## Repository Structure

```text
.
├── .github/workflows/
├── data/json/
├── genai_news_pipeline/
│   ├── pipelines.py
│   ├── settings.py
│   └── spiders/
└── scripts/
```

Key directories:

- `genai_news_pipeline/`: the Scrapy project
- `scripts/`: entry scripts for scraping, enrichment, aggregation, and syncing
- `data/json/`: prompts and helper data used by the aggregation stage
- `.github/workflows/`: GitHub Actions workflows

## Environment Variables

See [`.env.example`](.env.example):

```env
# MongoDB
MONGO_URI=
MONGO_DATABASE=
MONGO_COLLECTIONS=mothership,straits_times,channel_news_asia

# X / Twitter API
# [optional]
TARGET_ACCOUNTS=
TWITTER_API_KEY=

# Gemini API
GEMINI_API_KEY=
```

Notes:

- `MONGO_COLLECTIONS` and `TARGET_ACCOUNTS` are both comma-separated lists
- Their order must match one-to-one
- `run_spiders.py`, `enrich_articles.py`, and `aggregate_stories.py` require MongoDB
- `enrich_articles.py` and `aggregate_stories.py` require Gemini API access
- `[optional] sync_tweets.py` also requires `TWITTER_API_KEY`

## Local Development

Recommended Python version: `3.11`

Install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Prepare environment variables:

```bash
cp .env.example .env
```

Run the main stages manually:

```bash
python scripts/run_spiders.py
python scripts/enrich_articles.py
python scripts/aggregate_stories.py
```

To sync X accounts [optional]:

```bash
python scripts/sync_tweets.py
```

## GitHub Actions

The scheduled workflow is [`.github/workflows/news_pipeline.yml`](.github/workflows/news_pipeline.yml).

It runs once per hour from `08:35` to `20:35` Singapore time and executes the following steps in a single job:

1. `scripts/run_spiders.py`
2. `scripts/enrich_articles.py`
3. `scripts/aggregate_stories.py`

Before running this workflow on GitHub Actions, add these repository secrets:

- `MONGO_URI`
- `MONGO_DATABASE`
- `MONGO_COLLECTIONS`
- `GEMINI_API_KEY`

Optional secrets for `sync_tweets.py` and `sync_tweets.yml`:

- `TARGET_ACCOUNTS`
- `TWITTER_API_KEY`

## Source Entry Points

- `https://mothership.sg/`
  Entry page for the Mothership spider.
- `https://www.straitstimes.com/_plat/api/v1/articlesListing?pageType=section&searchParam=singapore&page={page}&maxDate={max_date}`
  Listing API used by the Straits Times spider.
- `https://www.channelnewsasia.com/api/v1/infinitelisting/424d200d-65b8-46e8-95f6-164946a38f8c?_format=json&viewMode=infinite_scroll_listing&page={page}`
  Listing API used by the CNA spider.

## Data Destinations

The pipeline mainly writes to:

- the collections listed in `MONGO_COLLECTIONS`: raw scrape results, article content, and enrichment results
- `hot_stories`: aggregated story outputs

`aggregate_stories.py` is the stage that ultimately updates or inserts into `hot_stories`.

## Dependencies

Core dependencies from [`requirements.txt`](requirements.txt):

- `scrapy`
- `pymongo`
- `requests`
- `python-dotenv`
- `gradio_client`
- `google-genai`
- `numpy`
- `networkx`
