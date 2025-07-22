import json
import argparse
from datetime import datetime
from nt import cpu_count
import ray
from ray.util.queue import Queue
from fetch_crawl_urls import UrlCrawler
from fetch_sitemap_urls import SitemapAnalyzer

# Initialize ray (Configure to use all available CPUs)
ray.init(num_cpus=None)

# Initialize the ray queue
data_queue = Queue()

# Initialize the arguments parser
# (python main.py <name> <url> --max_pages <number> --type <number>)
parser = argparse.ArgumentParser()
parser.add_argument(
    "name",
    help="Name of site to analyze"
)
parser.add_argument(
    "url",
    help="URL of site to analyze"
)
parser.add_argument(
    "--max_pages",
    help="Max pages to analyse",
    type=int,
    required=False)
parser.add_argument(
    "--type",
    help="Fetch type: 1 - Sitemap (Crawl fallback), 2 - Crawl only",
    type=int,
    required=False
)

# Helper function to write data to JSON file
def write_to_json(urls: list, type: str) -> None:
    args = parser.parse_args()
    name = args.name
    name = name.lower().replace(" ", "_")
    with open(f"{name}_{type}_urls.json", "w", encoding="utf-8") as f:
        json.dump(urls, f, indent=2, ensure_ascii=False)
        print(f"Saved URLs to {name}_{type}_urls.json")

# Main execution function
def main():
    try:

        # Get the `url` and `run_type` arguments
        args = parser.parse_args()
        url = args.url
        run_type = int(args.type) if args.type else 1

        # Print the start time
        print(f"--- Start time: {datetime.now().strftime('%H:%M:%S')} ---\n")

        # If `run type` is 2 - URL Crawler, then directly crawl the site
        if run_type == 2:
            with UrlCrawler(url, data_queue) as scraper:
                max_pages = int(args.max_pages) if args.max_pages else None
                scraped_links = scraper.scrape_all_links(max_pages)
                print(f"\nCollected {len(scraped_links)} URLs for analysis")
                write_to_json(list(scraped_links), "crawled")

                queue_data = []
                while not data_queue.empty():
                    url, content = data_queue.get()
                    queue_data.append({"url": url, "content": content})
                    with open('queue_dump.json', "w") as f:
                        json.dump(queue_data, f, indent=2)

        # Any other `run_type`, run the analysis with sitemap urls
        # with crawling as fallback
        else:
            with SitemapAnalyzer(url, max_workers=5) as analyzer:
                urls, success = analyzer.get_all_urls()

                # If `sitemap` URls success then continue with analysis
                if success:
                    print(f"\nCollected {len(urls)} URLs for analysis")
                    write_to_json(list(urls), "sitemap")

                # If `sitemap` URls failed then start crawling for links
                # then proceed to analysis
                else:
                    print(f"Sitemap ineffective. Crawling {url} for links.")
                    with UrlCrawler(url, data_queue) as scraper:
                        max_pages = int(args.max_pages) if args.max_pages else None
                        scraped_links = scraper.scrape_all_links(max_pages)
                        print(f"\nCollected {len(scraped_links)} URLs for analysis")
                        write_to_json(list(scraped_links), "crawled")

        # Print the exit time
        print(f"\n--- End time: {datetime.now().strftime('%H:%M:%S')} ---")

    # Show message if keyboard interuption
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")

    # Show error message on any error catched
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
