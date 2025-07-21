import json
import argparse
from datetime import datetime
from fetch_crawl_urls import UrlCrawler
from fetch_sitemap_urls import SitemapAnalyzer

# Initialize the arguments parser
parser = argparse.ArgumentParser()
parser.add_argument("name", help="Name of site to analyze")
parser.add_argument("url", help="URL of site to analyze")
parser.add_argument("--max_pages", help="Max pages to analyse", type=int, required=False)

def write_to_json(urls: list, type: str) -> None:
    args = parser.parse_args()
    name = args.name
    name = name.lower().replace(" ", "_")
    with open(f"{name}_{type}_urls.json", "w", encoding="utf-8") as f:
        json.dump(urls, f, indent=2, ensure_ascii=False)
        print(f"Saved URLs to {name}_{type}_urls.json")

def main():
    try:
        args = parser.parse_args()
        url = args.url

        print(f"--- Start time: {datetime.now().strftime('%H:%M:%S')} ---\n")
        with SitemapAnalyzer(url, max_workers=5) as analyzer:
            urls, success = analyzer.get_all_urls()

            if success:
                print(f"Found {len(urls)} webpage URLs")
                write_to_json(list(urls), "sitemap")
            else:
                print(f"Sitemap ineffective. Crawling {url} for links.")
                with UrlCrawler(url) as scraper:
                    max_pages = int(args.max_pages) if args.max_pages else None
                    scraped_links = scraper.scrape_all_links(max_pages)
                    write_to_json(list(scraped_links), "crawled")
        print(f"\n--- End time: {datetime.now().strftime('%H:%M:%S')} ---")
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
