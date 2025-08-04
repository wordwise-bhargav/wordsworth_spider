# Web Content Analysis Platform

The Web Content Analysis Platform is a sophisticated Python-based solution designed for the comprehensive extraction, scraping, and in-depth linguistic analysis of website content. Leveraging the power of `asyncio` for asynchronous operations and `Ray` for distributed computing, this platform ensures efficient and scalable processing of large volumes of web data.

## Core Capabilities

-   **Intelligent URL Discovery**: The platform prioritizes the discovery of URLs via `robots.txt` and sitemaps (including gzipped and sitemap index formats) to efficiently map out website structure. In scenarios where sitemaps are absent or incomplete, it seamlessly transitions to a robust web crawling mechanism, intelligently navigating links within the designated domain to ensure thorough coverage.
-   **High-Performance Concurrent Scraping**: Employing `aiohttp` for asynchronous HTTP requests and `Ray` actors for parallel processing, the system can concurrently fetch and parse content from numerous URLs. This architecture significantly accelerates the data acquisition phase, making it suitable for large-scale web content collection.
-   **Rich Media Metadata Extraction**: Beyond text, the platform extracts valuable image URLs along with their associated `alt` attributes, providing crucial metadata for accessibility and content understanding, and noting the source page of each image.
-   **Advanced Linguistic Analysis**: A key feature of this platform is its ability to perform detailed language detection on scraped text content. It identifies various languages, including a strong focus on Indian languages. The analysis provides a granular breakdown of word counts and percentages for each detected language, offering insights into the linguistic composition of web pages and the entire site.
-   **Robust Error Management and Adaptive Control**: The system is engineered for resilience, incorporating multiple retry mechanisms, configurable timeouts, and dynamic concurrency adjustments. This adaptive control helps in gracefully handling network latencies, HTTP errors, and unresponsive servers, ensuring high data acquisition success rates.
-   **Structured Data Output**: All collected and analyzed data is meticulously organized and saved into industry-standard formats. Scraped content, image details, and comprehensive language analysis reports are outputted as JSONL and JSON files, facilitating easy integration with other data processing and analytical tools.

## Architectural Overview

The platform is modular, with distinct components handling specific functionalities:

-   `main.py`: Serves as the primary orchestration module. It parses command-line arguments, initializes the `Ray` distributed environment, and coordinates the sequential execution of URL discovery, web scraping, and language analysis phases.
-   `fetch_sitemap_urls.py`: Dedicated to intelligent sitemap analysis. This module efficiently discovers sitemap locations from `robots.txt` and common paths, fetches sitemap (including gzipped) and sitemap index files, and parses them to extract a comprehensive list of URLs, while applying domain and file type filtering.
-   `sitemap_urls_crawler.py`: Manages the concurrent scraping of URLs obtained from the sitemap or crawling process. It utilizes `Ray` actors and `aiohttp` to asynchronously fetch page content, extract images, and record any errors, with an adaptive concurrency mechanism based on success rates.
-   `fetch_crawl_urls.py`: Acts as a robust fallback web crawler. If sitemap discovery is unsuccessful, this module systematically crawls web pages, extracts internal links, and adds them to the processing queue, respecting domain boundaries and avoiding non-content assets.
-   `language_analyzer.py`: Performs the core linguistic processing. It takes the scraped text content, cleans it, splits it into chunks for accurate language detection using `langdetect`, and then aggregates language statistics at both page and overall site levels.
-   `requirements.txt`: Lists all external Python libraries required for the project, ensuring easy and consistent environment setup.

## Getting Started

To set up and run the Web Content Analysis Platform, follow these steps:

### Prerequisites

-   Python 3.8+ installed.
-   Internet connectivity for web scraping.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd web-content-analyzer
    ```

2.  **Create and activate a virtual environment (recommended for dependency isolation):**
    ```bash
    python -m venv venv
    ```

    Activate the environment:
    ```bash
    source venv/bin/activate  # Linux
    ```
    ```bash
    venv\Scripts\activate     # Windows
    ```

3.  **Install the required Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note: The `ray` library may have additional system-level dependencies depending on your operating system. Refer to the official Ray documentation for specific installation notes if you encounter issues.*

### Execution

Run the `main.py` script from your terminal, providing the site name, URL, and an optional page limit:

```bash
python main.py <site_name> <site_url> [--max_pages <number_of_pages>]
````

  - `<site_name>`: A user-friendly string to name the analysis output files (e.g., "Corporate\_Website"). This should be a single word or words separated by underscores to avoid file naming issues.
  - `<site_url>`: The absolute URL of the target website (e.g., "https://www.example.com/"). The script will automatically normalize this URL.
  - `--max_pages <number_of_pages>` (optional): An integer specifying the maximum number of unique web pages to crawl if sitemap analysis is ineffective or yields limited results. If omitted, the crawler will attempt to collect all discoverable links within the specified domain without an explicit page limit.

#### Usage Examples:

1.  **Standard Analysis (Sitemap first, then crawl if needed, no page limit):**

    ```bash
    python main.py "Global_News" "[https://www.globalnews.com/](https://www.globalnews.com/)"
    ```

2.  **Limited Crawl Analysis (Sitemap first, then crawl up to 500 pages if necessary):**

    ```bash
    python main.py "Tech_Blog" "[https://www.techblog.net/](https://www.techblog.net/)" --max_pages 500
    ```

## Output Artifacts

Upon successful execution, the `outputs/` directory will contain the following files:

  - `<site_name>_sitemap_urls.json` or `<site_name>_crawled_urls.json`: A JSON formatted file detailing all unique URLs identified either through sitemap parsing or web crawling, serving as the input for the scraping phase.
  - `<site_name>_site_data.jsonl`: A JSON Lines file, where each line represents a scraped web page, containing its URL and the extracted, cleaned text content.
  - `<site_name>_images_urls.jsonl`: A JSON Lines file, with each line detailing an extracted image, including its URL, associated `alt` text (if available), and the URL of the source page where it was found.
  - `<site_name>_errors.jsonl`: A JSON Lines file that logs any URLs that could not be successfully processed, along with the encountered error or exception type.
  - `<site_name>_language_analysis.json`: A comprehensive JSON file presenting the linguistic analysis results. It includes per-page language breakdowns (word counts and percentages) and an aggregated site-wide language distribution summary, offering insights into the primary languages used across the website.

## Error Handling and Logging

The platform incorporates robust error handling to manage network interruptions, HTTP status errors, and parsing issues. Detailed warnings and errors are logged to the console and to the `_errors.jsonl` file to assist in debugging and understanding processing failures. `KeyboardInterrupt` is also gracefully handled, allowing for safe termination of the crawling process.
