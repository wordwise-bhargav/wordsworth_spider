# Indian Language Web Analyzer

A high-performance, distributed website analyzer built using `Ray`, `asyncio`, and `aiohttp`. It scrapes sitemap-based or crawl-discovered pages and analyzes the content for Indian language distribution (including English) using fast, adaptive concurrency control.

## Features

- Concurrent HTML scraping using `Ray` + `aiohttp` + `asyncio`
- Adaptive throttling based on server response (backs off on high error rate)
- Language detection for 12 Indian languages + English
- Full site summary + per-page word distribution in JSON format
- Fail-safe retry logic with exponential backoff
- Automatically scales based on available CPU cores

## Supported Languages

- English (`en`)
- Hindi (`hi`)
- Bengali (`bn`)
- Telugu (`te`)
- Marathi (`mr`)
- Tamil (`ta`)
- Urdu (`ur`)
- Gujarati (`gu`)
- Kannada (`kn`)
- Malayalam (`ml`)
- Odia (`or`)
- Punjabi (`pa`)
- Assamese (`as`)

## Installation

1. Clone the repository
2. (Optional but recommended) Create a virtual environment:
```bash
    python -m venv .venv && source .venv/bin/activate   # (Linux/macOS)
```
or
```bash
    python -m venv .venv && .venv\Scripts\activate      # (Windows)
```
3. Install dependencies:
```bash
    pip install -r requirements.txt
```

## Usage

### Scraping + Saving HTML Text

Run the scraper with:

```bash
python main.py <sitename> <url>
```

This will:
- Fetch and parse the sitemap (or crawl if sitemap fails)
- Scrape pages concurrently
- Save:
  - `outputs/<sitename>_site_data.jsonl`    → scraped clean text content
  - `outputs/<sitename>_sitemap_urls.json`  → collected URLs
  - `outputs/<sitename>_errors.jsonl`       → any failed requests

You can also limit pages:

```bash
    python main.py <sitename> <url> --max_pages 50
```

## Language Analysis

After scraping:

Run language detection and summarization:

```bash
    python language_analysis.py
```

This will produce:

- `outputs/language_analysis.json`
  A valid JSON file including:
  - `pages`   → list of URLs and language stats
  - `summary` → full-site word count and language distribution

## Output Files

- `outputs/site_data.jsonl`           → scraped content from all pages
- `outputs/language_analysis.json`    → per-page and overall language distribution
- `outputs/scrape_errors.jsonl`       → failed requests with error type
- `outputs/<name>_sitemap_urls.json`  → URLs collected from sitemap

## Configuration Notes

- Uses dynamic actor allocation based on system CPU count
- Backoff & retry: 3 attempts with exponential delay
- Connector concurrency: `limit_per_host=2`
- Concurrency is automatically adjusted (throttled or scaled) during scraping

## Internals & Modules

- `sitemap_urls_crawler.py` → Fully adaptive Ray-based scraper
- `fetch_sitemap_urls.py`   → Sitemap parsing logic
- `language_analysis.py`    → Ray-parallelized language detection using langdetect
- `requirements.txt`        → All pinned dependencies

## Tested With

- Python 3.10+
- Ray 2.48+
- aiohttp 3.12+
- tqdm, BeautifulSoup, lxml, langdetect
