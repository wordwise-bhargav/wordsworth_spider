import os
import ray
import json
import asyncio
import argparse
import threading
from nt import cpu_count
from datetime import datetime
from ray.util.queue import Queue
from fetch_crawl_urls import UrlCrawler
from fetch_sitemap_urls import SitemapAnalyzer
from sitemap_urls_crawler import RayAsyncScraper
from language_analyzer import run_language_analysis

# Initialize ray (Configure to use all available CPUs)
ray.init(num_cpus=None)

# Initialize the ray queue
data_queue = Queue()

# Initialize the arguments parser
# (python main.py <name> <url> --max_pages <number>)
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

# Helper function to write data to JSON file
def write_to_json(urls: list, type: str) -> None:
    args = parser.parse_args()
    name = args.name.lower().replace(" ", "_")

    with open(f"outputs/{name}_{type}_urls.json", "w", encoding="utf-8") as f:
        json.dump(urls, f, indent=2, ensure_ascii=False)
        print(f"Saved URLs to {name}_{type}_urls.json")

# Main execution function
def main():
    try:
        # Get the `url` and `run_type` arguments
        args = parser.parse_args()
        url = args.url
        run_type = int(args.type) if args.type else 1

        name = args.name.lower().replace(" ", "_")
        stream_output_path = f"outputs/{name}_site_data.jsonl"
        stream_error_path = f"outputs/{name}_errors.jsonl"
        analysis_output_path = f"outputs/{name}_language_analysis.json"

        # Ensure output directory exists
        os.makedirs("outputs", exist_ok=True)

        # Print the start time
        print(f"--- Start time: {datetime.now().strftime('%H:%M:%S')} ---\n")

        # Start analysis from sitemap scraping
        with SitemapAnalyzer(url, max_workers=5) as analyzer:
            urls, success = analyzer.get_all_urls()

            # If `sitemap` URls success then continue with analysis
            if success:
                print(f"\nCollected {len(urls)} URLs for analysis")
                write_to_json(list(urls), "sitemap")

                scraper = RayAsyncScraper(
                    urls=list(urls),
                    output_file=stream_output_path,
                    error_file=stream_error_path,
                    batch_size=20,
                )
                asyncio.run(scraper.scrape())

            # If `sitemap` URls failed then start crawling for links
            # then proceed to analysis
            else:
                print(f"Sitemap ineffective. Crawling {url} for links.")
                with UrlCrawler(url, stream_output_path) as scraper:
                    max_pages = int(args.max_pages) if args.max_pages else None
                    scraped_links = scraper.scrape_all_links(max_pages)
                    print(f"\nCollected {len(scraped_links)} URLs for analysis")
                    write_to_json(list(scraped_links), "crawled")

        # Run languages analysis
        asyncio.run(run_language_analysis(
            stream_output_path,
            analysis_output_path
        ))

        # Print the exit time
        print(f"\n--- End time: {datetime.now().strftime('%H:%M:%S')} ---")

    # Show message if keyboard interuption
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")

    # Show error message on any error catched
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        ray.shutdown()

if __name__ == "__main__":
    main()
