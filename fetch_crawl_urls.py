import os
import re
import ray
import sys
import time
import asyncio
import aiohttp
import requests
import jsonlines
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque
from urllib.parse import urljoin, urlparse, unquote, quote

# Set of file extensions to ignore (assets)
IGNORE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".css", ".js", ".woff", ".ttf", ".eot", ".pdf",
    ".zip", ".mp4", ".mp3", ".webm", ".xml", ".json",
    ".woff2", ".map", ".txt", ".doc", ".docx", ".xlsx"
}

# Configuration constants
MAX_CONCURRENT_REQUESTS = 50
BATCH_SIZE = 100
REQUEST_TIMEOUT = 10
MAX_RETRIES = 2

MAX_LINE = "=> To Process: 99999 | Total Collected: 99999 | Rate: 999.9 URLs/s"
MAX_WIDTH = len(MAX_LINE)

@ray.remote
class OptimizedVisitedSet:
    """Ray remote class for managing visited URLs with batch operations."""

    def __init__(self):
        self.visited = set()

    def contains_batch(self, urls):
        """Check multiple URLs at once."""
        return [url in self.visited for url in urls]

    def add_batch(self, urls):
        """Add multiple URLs at once, returning only newly added URLs."""
        new_urls = []
        for url in urls:
            if url not in self.visited:
                self.visited.add(url)
                new_urls.append(url)
        return new_urls

    def contains(self, url):
        """Check if a single URL is visited."""
        return url in self.visited

    def add(self, url):
        """Add a single URL, return True if it was new."""
        if url not in self.visited:
            self.visited.add(url)
            return True
        return False

    def size(self):
        """Get the total number of visited URLs."""
        return len(self.visited)

    def get_all(self):
        """Get all visited URLs as a list."""
        return list(self.visited)

class UrlCrawler:
    """Optimized web crawler using Ray for parallel processing and async I/O."""

    def __init__(self, url, file_name):
        self.url = url
        self.file_name = file_name

    def format_status(self, to_process, total_collected, rate=0.0):
        """Format the status line for progress display."""
        line = f"=> To Process: {to_process} | Total Collected: {total_collected} | Rate: {rate:.1f} URLs/s"
        return line.ljust(MAX_WIDTH)

    def normalize_netloc(self, netloc):
        """Normalize network location by removing www prefix and converting to lowercase."""
        return netloc.lower().removeprefix("www.")

    def normalize_url(self, url):
        """Normalize URL with proper percent-encoding handling."""
        try:
            parsed = urlparse(url)

            # Normalize domain (case-insensitive)
            clean_netloc = self.normalize_netloc(parsed.netloc)

            # Normalize percent-encoding in path
            try:
                decoded_path = unquote(parsed.path)
                normalized_path = quote(decoded_path, safe='/')

                # Handle trailing slashes consistently
                if normalized_path == '/':
                    normalized_path = ''
                elif normalized_path.endswith('/'):
                    normalized_path = normalized_path.rstrip('/')

            except (UnicodeDecodeError, UnicodeEncodeError):
                # Fallback to original path if encoding issues
                normalized_path = parsed.path.rstrip('/') if parsed.path != '/' else ''

            return f"{parsed.scheme}://{clean_netloc}{normalized_path}"

        except Exception:
            return url  # Return original URL if parsing fails

    def should_ignore_url(self, url):
        """Check if URL should be ignored based on file extension."""
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in IGNORE_EXTENSIONS)

    async def crawl_single_url(self, session, url, base_netloc, visited_actor):
        """Crawl a single URL with async I/O."""
        new_links = set()

        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return new_links

                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type:
                    return new_links

                # Read content with size limit (50MB)
                content = await response.read()
                if len(content) > 50 * 1024 * 1024:
                    return new_links

                soup = BeautifulSoup(content, "html.parser")
                links_to_check = []

                # Add the extracted data to the common file for analysis
                with jsonlines.open(self.file_name, mode='a') as writer:
                    writer.write({"url": url, "value": soup.get_text(separator="\n", strip=True)})

                for a_tag in soup.find_all("a", href=True):
                    href = a_tag['href'] # type: ignore
                    if not href or href.startswith('#') or href.startswith('mailto:') or href.startswith('tel:'): # type: ignore
                        continue

                    joined_url = urljoin(url, href) # type: ignore
                    parsed = urlparse(joined_url)

                    # Quick validation
                    if parsed.scheme not in {"http", "https"}:
                        continue

                    clean_netloc = self.normalize_netloc(parsed.netloc)
                    if clean_netloc != base_netloc:
                        continue

                    if self.should_ignore_url(joined_url):
                        continue

                    clean_url = self.normalize_url(joined_url)
                    links_to_check.append(clean_url)

                # Batch check for visited URLs
                if links_to_check:
                    visited_status = ray.get(visited_actor.contains_batch.remote(links_to_check))
                    new_urls = [url for url, is_visited in zip(links_to_check, visited_status) if not is_visited]

                    if new_urls:
                        actually_new = ray.get(visited_actor.add_batch.remote(new_urls))
                        new_links.update(actually_new)

        except Exception:
            # Silently handle exceptions in production
            pass

        return new_links

    async def _async_crawl_batch(self, urls, base_netloc, visited_actor):
        """Internal async function for batch crawling."""
        new_links = set()

        # Create session with optimized settings
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REQUESTS,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; WebCrawler/1.0)'}
        ) as session:

            # Process with controlled concurrency
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

            async def crawl_with_semaphore(url):
                async with semaphore:
                    return await self.crawl_single_url(session, url, base_netloc, visited_actor)

            tasks = [crawl_with_semaphore(url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, set):
                    new_links.update(result)

        return new_links

    def sync_crawl_single_url(self, url, base_netloc, visited_actor):
        """Synchronous crawling of a single URL as fallback."""
        new_links = set()

        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                return new_links

            if 'text/html' not in response.headers.get('Content-Type', ''):
                return new_links

            soup = BeautifulSoup(response.text, "html.parser")
            links_to_check = []

            # Add the extracted data to the common file for analysis
            with jsonlines.open(self.file_name, mode='a') as writer:
                writer.write({"url": url, "value": soup.get_text(separator="\n", strip=True)})

            for a_tag in soup.find_all("a", href=True):
                href = a_tag['href'] # type: ignore
                if not href or href.startswith('#') or href.startswith('mailto:') or href.startswith('tel:'): # type: ignore
                    continue

                joined_url = urljoin(url, href) # type: ignore
                parsed = urlparse(joined_url)

                if parsed.scheme not in {"http", "https"}:
                    continue

                clean_netloc = self.normalize_netloc(parsed.netloc)
                if clean_netloc != base_netloc:
                    continue

                if self.should_ignore_url(joined_url):
                    continue

                clean_url = self.normalize_url(joined_url)
                links_to_check.append(clean_url)

            # Batch check for visited URLs
            if links_to_check:
                visited_status = ray.get(visited_actor.contains_batch.remote(links_to_check))
                new_urls = [url for url, is_visited in zip(links_to_check, visited_status) if not is_visited]

                if new_urls:
                    actually_new = ray.get(visited_actor.add_batch.remote(new_urls))
                    new_links.update(actually_new)

        except Exception:
            # Silently handle exceptions in production
            pass

        return new_links

    @ray.remote(num_cpus=0.2)
    def async_crawl_batch(self, urls, base_netloc, visited_actor):
        """Ray remote function for asynchronous batch crawling."""
        return asyncio.run(self._async_crawl_batch(urls, base_netloc, visited_actor))

    @ray.remote
    def sync_crawl_batch(self, urls, base_netloc, visited_actor):
        """Ray remote function for synchronous batch crawling as fallback."""
        new_links = set()

        for url in urls:
            url_links = self.sync_crawl_single_url(url, base_netloc, visited_actor)
            new_links.update(url_links)

        return new_links

    def scrape_all_links(self, max_pages=None):
        """Main crawling method with optimized batching and async I/O."""
        start_url = self.normalize_url(self.url)
        parsed_base = urlparse(start_url)
        base_netloc = self.normalize_netloc(parsed_base.netloc)
        visited = OptimizedVisitedSet.remote()

        # Initialize with start URL
        ray.get(visited.add.remote(start_url)) # type: ignore
        to_crawl = deque([start_url])
        all_results = {start_url}

        start_time = time.time()
        last_update = start_time

        while to_crawl and (max_pages is None or len(all_results) < max_pages):
            # Prepare batch
            current_batch = []
            batch_size = min(BATCH_SIZE, len(to_crawl))

            for _ in range(batch_size):
                if to_crawl:
                    current_batch.append(to_crawl.popleft())

            if not current_batch:
                break

            # Process batch with async crawling (with fallback)
            try:
                future = self.async_crawl_batch.remote(self, current_batch, base_netloc, visited)
                new_links = ray.get(future)
            except Exception as e:
                # Fallback to sync if async fails
                print(f"\nAsync crawling failed, falling back to sync: {e}")
                future = self.sync_crawl_batch.remote(self, current_batch, base_netloc, visited)
                new_links = ray.get(future)

            # Add new links to queue and results
            for link in new_links:
                if link not in all_results:
                    all_results.add(link)
                    to_crawl.append(link)

            # Update progress display
            current_time = time.time()
            if current_time - last_update >= 1.0:  # Update every second
                elapsed = current_time - start_time
                rate = len(all_results) / elapsed if elapsed > 0 else 0
                print(self.format_status(len(to_crawl), len(all_results), rate), end="\r", flush=True)
                last_update = current_time

        return sorted(all_results)

    def close(self):
        return

    def __enter__(self):
        # Initialize Ray with optimized configuration
        if not ray.is_initialized():
            ray.init(num_cpus=None)  # Use all available CPUs

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
