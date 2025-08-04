# 🕷️ Web Content Analyzer

A fully automated and adaptive pipeline to:
- Crawl websites or extract URLs from sitemaps
- Scrape webpage content and image URLs
- Detect and analyze language distribution across pages
- Output rich structured analytics

Built using **Ray**, **asyncio**, and **aiohttp**, this project supports English and 12 Indian languages, scales across CPUs, and handles real-world websites with adaptive fault-tolerance.

---

## 📦 Features

- 🌐 Sitemap-aware scraping with fallback to full crawling
- ⚡ High-speed, concurrency-adaptive async scraping
- 🖼️ Image URL collection from all pages
- 🧠 Language analysis per page using `langdetect`
- 📊 Overall summary of languages and word counts
- 💻 CLI & programmatic usage, optimized with Ray multiprocessing

---

## 🛠️ Installation

> Recommended: Use a virtual environment with [uv](https://github.com/astral-sh/uv)

```bash
# Clone the repository
git clone https://github.com/your-org/your-repo.git
cd your-repo

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
uv pip install -r requirements.txt
````

### ✅ Requirements

Dependencies are listed in `requirements.txt`, and include:

* `aiohttp`
* `beautifulsoup4`
* `jsonlines`
* `langdetect`
* `lxml`
* `requests`
* `ray`
* `tqdm`

---

## 🚀 CLI Usage

You can run the entire analysis from the command line:

```bash
python site_analyzer.py <name> <url> [--max_pages <N>]
```

### Arguments

| Arg           | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| `name`        | Name of the site or brand (used in output filenames)         |
| `url`         | The root URL of the website to analyze                       |
| `--max_pages` | (Optional) Fallback page limit for crawling if sitemap fails |

### Example

```bash
python site_analyzer.py Wordwise https://www.wordwise.one/ --max_pages 500
```

---

## 📁 Output Files

All output is saved to the `outputs/` directory:

| File                            | Description                                            |
| ------------------------------- | ------------------------------------------------------ |
| `<name>_site_data.jsonl`        | Scraped text content for each URL                      |
| `<name>_images_urls.jsonl`      | All image URLs encountered                             |
| `<name>_language_analysis.json` | Page-wise and overall language statistics              |
| `<name>_sitemap_urls.json`      | URLs extracted from sitemap                            |
| `<name>_crawled_urls.json`      | URLs collected via crawling (if sitemap fails)         |
| `<name>_errors.jsonl`           | Pages that failed during scraping, with exception info |

---

## 🧩 Programmatic Usage

### 🔁 High-Level Usage (One Call)

Use `start_analysis()` to run the **entire pipeline** in one line:

```python
from site_analyzer import start_analysis

results = start_analysis(
    brand_name="Wordwise",
    url="https://www.wordwise.one/",
    max_pages=500  # Optional fallback limit
)
```

This will:

* Try to extract URLs from sitemap
* Crawl pages if sitemap is missing or fails
* Scrape page content and images
* Run per-page and overall language detection
* Write results to the `outputs/` folder
* Returns the summary of the analysis

---

### 🧩 Component Usage

#### Sitemap Extraction

```python
from fetch_sitemap_urls import SitemapAnalyzer

with SitemapAnalyzer("https://example.com") as analyzer:
    urls, found = analyzer.get_all_urls()
```

#### URL Crawler (Fallback)

```python
from fetch_crawl_urls import UrlCrawler

with UrlCrawler(start_url, text_file, image_file, error_file) as crawler:
    links = crawler.scrape_all_links(max_pages=500)
```

#### Async Scraper (Ray-based)

```python
from sitemap_urls_crawler import RayAsyncScraper
import asyncio

scraper = RayAsyncScraper(
    urls=urls,
    output_file="outputs/scraped.jsonl",
    images_file="outputs/images.jsonl",
    error_file="outputs/errors.jsonl",
)
asyncio.run(scraper.scrape())
```

#### Language Analysis

```python
from language_analyzer import run_language_analysis
import asyncio

asyncio.run(run_language_analysis(
    input_path="outputs/scraped.jsonl",
    output_path="outputs/lang_summary.json"
))
```

---

## 📊 Supported Languages

This tool detects content in the following languages:

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

## ⚙️ Tuning for Large Sites

This tool handles large sites (100k+ pages) via:

* 🧠 Adaptive concurrency (automatically scales up/down actors based on error rate)
* ✅ Ray-based parallelism based on available CPUs
* 💾 Streamed JSONL output to avoid memory overload

You can fine-tune:

* `max_actors`, `min_actors`, and `batch_size` in `RayAsyncScraper`
* `--max_pages` if sitemap fails
* Ray cluster settings if scaling across nodes

> No manual tuning is required for most real-world websites. However, if scraping hundreds of thousands of pages, consider horizontal scaling or enabling Ray autoscaling.

---

## 🧹 Cleanup

To reset outputs:

```bash
rm -rf outputs/*
```

---

## 🧠 Behind the Scenes

* 🌐 Sitemap-aware discovery with robots.txt fallback
* ⚙️ Fully async scraping with exponential backoff
* 📈 Language chunking and per-word classification
* 💾 Streamed JSONL output for scalable storage
* 🧠 Ray-powered concurrency using `ray.remote` actors

---
