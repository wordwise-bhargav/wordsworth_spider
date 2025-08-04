# 🕷️ Web Content Analyzer

A fully automated and adaptive pipeline to:

* Crawl websites or extract URLs from sitemaps
* Scrape text and image URLs
* Detect and analyze language distribution across pages
* Output rich, structured analytics

Supports English and 12 Indian languages with intelligent crawling and fault-tolerant scraping using **Ray**, **asyncio**, and **aiohttp**.

---

## 📦 Features

* 🌐 Sitemap-aware and fallback crawler
* ⚡ High-speed async scraping with dynamic concurrency throttling
* 🖼️ Image URL extraction
* 🧠 Language detection on scraped content
* 📊 Summary reports per page and overall
* 🧵 Multiprocessing and distributed execution via **Ray**

---

## 🛠️ Installation

> Recommended: Use a virtual environment with [uv](https://github.com/astral-sh/uv)

```bash
# Clone the repo
git clone https://github.com/your-org/your-repo.git
cd your-repo

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
uv pip install -r requirements.txt
```

### ✅ Requirements

The core packages used include:

* `aiohttp`
* `beautifulsoup4`
* `jsonlines`
* `langdetect`
* `ray`
* `tqdm`
* `lxml`
* `requests`

> You can find all required packages in `requirements.txt`.

---

## 🚀 CLI Usage

You can directly run the analyzer from CLI using:

```bash
python site_analyzer.py <name> <url> [--max_pages <N>]
```

### Parameters

* `name`: Brand/site name (used for output filenames)
* `url`: Starting URL of the website
* `--max_pages`: (Optional) Limit for number of pages to crawl if sitemap fails

### Example

```bash
python site_analyzer.py Wordwise https://www.wordwise.one/ --max_pages 500
```

---

## 📁 Output Files

All results are saved under the `outputs/` directory:

| File                            | Description                           |
| ------------------------------- | ------------------------------------- |
| `<name>_site_data.jsonl`        | Scraped text content per page         |
| `<name>_images_urls.jsonl`      | All image URLs discovered             |
| `<name>_language_analysis.json` | Language distribution summary         |
| `<name>_sitemap_urls.json`      | URLs from sitemap (if available)      |
| `<name>_crawled_urls.json`      | URLs from crawling (if sitemap fails) |
| `<name>_errors.jsonl`           | Failed fetches with error details     |

---

## 🧩 Programmatic Usage

You can use the core components as importable Python modules:

### Crawl Fallback

```python
from fetch_crawl_urls import UrlCrawler

with UrlCrawler(url, output_jsonl, image_jsonl, error_file) as crawler:
    all_links = crawler.scrape_all_links(max_pages=500)
```

### Sitemap Extraction

```python
from fetch_sitemap_urls import SitemapAnalyzer

with SitemapAnalyzer("https://example.com") as analyzer:
    urls, found = analyzer.get_all_urls()
```

### Async Ray-Based Scraper

```python
from sitemap_urls_crawler import RayAsyncScraper
import asyncio

scraper = RayAsyncScraper(
    urls=urls,
    output_file="outputs/data.jsonl",
    images_file="outputs/images.jsonl",
    error_file="outputs/errors.jsonl"
)
asyncio.run(scraper.scrape())
```

### Language Analysis

```python
from language_analyzer import run_language_analysis
import asyncio

asyncio.run(run_language_analysis(
    input_path="outputs/data.jsonl",
    output_path="outputs/language_summary.json"
))
```

---

## 🧪 Development and Testing

You can test individual modules by running them directly. Example:

```bash
python language_analyzer.py
```

Or manually import and test functions in a notebook or interactive console.

---

## 📊 Supported Languages

The system detects the following languages using `langdetect`:

* English
* Hindi
* Bengali
* Telugu
* Marathi
* Tamil
* Urdu
* Gujarati
* Kannada
* Malayalam
* Odia
* Punjabi
* Assamese

---

## 🧹 Cleanup

To reset outputs:

```bash
rm -rf outputs/*
```

---

## 🧠 Behind the Scenes

* **Adaptive concurrency:** Sitemap scraping dynamically reduces Ray actor count on error spikes.
* **Normalized crawling:** Handles asset filtering, malformed links, netloc constraints.
* **Resilient:** Handles encoding issues, huge files, timeouts, broken pages.

---

## ✅ Tips

* Use `--max_pages` only when the sitemap fails.
* Use `ray.init(num_cpus=<n>)` in script to limit CPU usage if needed.
* Avoid using this on very large sites unless concurrency limits are tuned.
