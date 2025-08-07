import os
import ray
import json
import asyncio
import argparse
import threading
from nt import cpu_count
from datetime import datetime
from ray.util.queue import Queue

# Import normally if main or not package
if __name__ == "__main__" or __package__ is None:
    from fetch_crawl_urls import UrlCrawler
    from fetch_sitemap_urls import SitemapAnalyzer
    from sitemap_urls_crawler import RayAsyncScraper
    from language_analyzer import run_language_analysis
    from image_analyzer import run_image_analysis

# Import relative path if package usage
else:
    from .fetch_crawl_urls import UrlCrawler
    from .fetch_sitemap_urls import SitemapAnalyzer
    from .sitemap_urls_crawler import RayAsyncScraper
    from .language_analyzer import run_language_analysis
    from .image_analyzer import run_image_analysis

# Initialize ray (Configure to use all available CPUs)
ray.init(num_cpus=None)

# Initialize the ray queue
data_queue = Queue()

# Helper function to write data to JSON file
def write_to_json(urls: list, type: str) -> None:
    args = parser.parse_args()
    name = args.name.lower().replace(" ", "_")

    with open(f"outputs/{name}_{type}_urls.json", "w", encoding="utf-8") as f:
        json.dump(urls, f, indent=2, ensure_ascii=False)
        print(f"Saved URLs to {name}_{type}_urls.json")

# Main execution function
def start_analysis(brand_name: str, url: str, max_pages: int | None = None) -> dict:
    try:
        name = brand_name.lower().replace(" ", "_")
        stream_output_path = f"outputs/{name}_site_data.jsonl"
        stream_error_path = f"outputs/{name}_errors.jsonl"
        analysis_output_path = f"outputs/{name}_language_analysis.json"
        images_output_path = f"outputs/{name}_images_urls.jsonl"
        image_analysis_output_path = f"outputs/{name}_image_analysis.json"

        # Ensure output directory exists
        os.makedirs("outputs", exist_ok=True)

        # Print the start time
        print(f"--- Start time: {datetime.now().strftime('%H:%M:%S')} ---\n")

        # Start analysis from sitemap scraping
        with SitemapAnalyzer(url, max_workers=5) as analyzer:
            urls, success = analyzer.get_all_urls()

            # If `sitemap` URls success then continue with analysis
            if success:
                print(f"Collected {len(urls)} URLs for analysis")
                write_to_json(list(urls), "sitemap")
                print("-----------------------------------\n")

                scraper = RayAsyncScraper(
                    urls=list(urls),
                    output_file=stream_output_path,
                    images_file=images_output_path,
                    error_file=stream_error_path,
                    batch_size=20,
                )
                asyncio.run(scraper.scrape())

            # If `sitemap` URls failed then start crawling for links
            # then proceed to analysis
            else:
                print(f"Sitemap ineffective. Crawling {url} for links.")
                with UrlCrawler(url, stream_output_path, images_output_path, stream_error_path) as scraper:
                    max_pages = int(max_pages) if max_pages else None
                    scraped_links = scraper.scrape_all_links(max_pages)
                    print("\n\n--- URLs scraping completed ---")
                    print(f"Collected {len(scraped_links)} URLs for analysis")
                    print("-------------------------------\n")
                    write_to_json(list(scraped_links), "crawled")

        # Run languages analysis
        text_results = asyncio.run(run_language_analysis(
            stream_output_path,
            analysis_output_path
        ))

        image_results = run_image_analysis(images_output_path, image_analysis_output_path)

        # Print the exit time
        print(f"\n--- End time: {datetime.now().strftime('%H:%M:%S')} ---")
        return {
            "text": text_results,
            "image": image_results
        }

    # Show message if keyboard interuption
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
        return {}

    # Show error message on any error catched
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}

    finally:
        ray.shutdown()

if __name__ == "__main__":
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
        required=False
    )

    # Get the `url` and `run_type` arguments
    args = parser.parse_args()
    name = args.name
    url = args.url
    results = start_analysis(name, url)
    print(json.dumps(results, indent=2, ensure_ascii=False))
