# Website URL Analyzer

This Python script helps you extract URLs from a website, either by analyzing its sitemap or by crawling its pages. It's useful for SEO analysis, content auditing, or simply getting a comprehensive list of pages on a given domain.

## Features

* **Sitemap Analysis:** Prioritizes fetching URLs from the site's `sitemap.xml` for a quick and efficient way to get a list of indexed pages.

* **Website Crawling:** If a sitemap is not found or is ineffective, the script falls back to crawling the website to discover links.

* **Configurable Page Limit:** When crawling, you can specify a maximum number of pages to analyze.

* **JSON Output:** Saves the extracted URLs into a JSON file for easy processing and storage.

## Prerequisites

This script relies on two external modules: `fetch_crawl_urls` and `fetch_sitemap_urls`. You would typically have these as separate Python files (`fetch_crawl_urls.py` and `fetch_sitemap_urls.py`) in the same directory as this script.

All required dependencies can be installed from the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

## How to Run

The script is executed from the command line and requires the site's name and URL as arguments.

### Basic Usage

```bash
python your_script_name.py --type <number[1 or 2]> "Site Name" "https://example.com"
```

* `your_script_name.py`: Replace this with the actual name of your Python script (e.g., `main.py`).

* `<number>`: Replace with either 1 - Sitemap with crawl fallback or 2 - Crawl only

* `"Site Name"`: A descriptive name for the website (e.g., `"My Blog"`). This will be used in the output JSON filename.

* `"https://example.com"`: The full URL of the website you want to analyze.

### With Max Pages (for crawling)

If the sitemap is ineffective or you explicitly want to limit the crawl, use the `--max_pages` argument:

```bash
python your_script_name.py "My Blog" "https://myblog.com" --max_pages 100
```

This will limit the crawler to a maximum of 100 pages.

## Examples

1. **Analyze a website using its sitemap (if available):**

```bash
python main.py "Google" "https://www.google.com"
```

This will attempt to find and parse `https://www.google.com/sitemap.xml` (or similar sitemap locations). If successful, it will save the URLs to `google_sitemap_urls.json`.

2. **Crawl a website and limit to 50 pages:**

```bash
python main.py "Tech News" "https://technews.com" --max_pages 50
```

If `technews.com` doesn't have an accessible sitemap, or if the sitemap analysis fails, the script will start crawling `technews.com` and stop after visiting 50 unique pages. The URLs will be saved to `tech_news_crawled_urls.json`.

## Output

The script will create a JSON file in the same directory where it's run. The filename will follow the pattern: `<site_name>_<type>_urls.json`.

* `<site_name>`: The name you provided, converted to lowercase and spaces replaced with underscores (e.g., `google`, `tech_news`).

* `<type>`: Either `sitemap` (if URLs were found via sitemap) or `crawled` (if URLs were found via crawling).

The JSON file will contain a list of URLs, for example:

```json
[
"https://example.com/",
"https://example.com/about",
"https://example.com/contact",
"https://example.com/blog/article-1",
"https://example.com/blog/article-2"
]
```


## Error Handling

The script includes basic error handling for `KeyboardInterrupt` (if you stop the script manually) and other general exceptions during execution.
