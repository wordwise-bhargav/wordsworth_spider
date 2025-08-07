import os
import ray
import json
import time
import asyncio
import aiohttp
from tqdm import tqdm
from typing import List
from pathlib import Path
from bs4 import BeautifulSoup

@ray.remote
class AsyncAiohttpFetcher:
    def __init__(self, images_path: Path, timeout: int = 10, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.images_path = images_path

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> dict:
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(url) as response:
                    text = await response.text()
                    soup = BeautifulSoup(text, "lxml")
                    content = soup.get_text(separator="\n", strip=True)

                    with self.images_path.open("a", encoding="utf-8") as f:
                        for img in soup.find_all("img", src=True):
                            f.write(json.dumps(img["src"], ensure_ascii=False) + "\n")

                    return {
                        "url": url,
                        "status": response.status,
                        "content": content
                    }
            except Exception as e:
                if attempt == self.max_retries:
                    return {
                        "url": url,
                        "error": str(e),
                        "exception_type": type(e).__name__
                    }
                await asyncio.sleep(2 ** attempt)  # exponential backoff

    async def fetch_single(self, url: str) -> dict:
        """Fetch a single URL - for individual progress tracking"""
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; SitemapBot/1.0)"
        }
        connector = aiohttp.TCPConnector(limit_per_host=2)

        async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
            return await self._fetch(session, url)

    async def fetch(self, urls: List[str]) -> List[dict]:
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; SitemapBot/1.0)"
        }
        connector = aiohttp.TCPConnector(limit_per_host=2)

        async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
            tasks = [self._fetch(session, url) for url in urls]
            return await asyncio.gather(*tasks)

class RayAsyncScraper:
    def __init__(
        self,
        urls: List[str],
        output_file: str = "scraped_output.jsonl",
        images_file: str = "scraped_images.jsonl",
        error_file: str = "scrape_errors.jsonl",
        batch_size: int = 20,
        max_actors: int = 8,
        min_actors: int = 1,
        throttle_threshold: float = 0.15  # If >15% fail, reduce concurrency
    ):
        self.urls = urls
        self.output_path = Path(output_file)
        self.images_file = Path(images_file)
        self.error_path = Path(error_file)
        self.batch_size = batch_size
        self.max_actors = min(max_actors, os.cpu_count() or 2) if max_actors else os.cpu_count() or 2
        self.min_actors = min_actors
        self.throttle_threshold = throttle_threshold
        self.cur_actors = max_actors

    def _split_batches(self) -> List[List[str]]:
        return [self.urls[i:i + self.batch_size] for i in range(0, len(self.urls), self.batch_size)]

    def _stream_to_jsonl(self, path: Path, data: List[dict]):
        with path.open("a", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    async def scrape(self):
        print(f"Starting scrape with up to {self.max_actors} Ray actors")

        total = len(self.urls)
        progress = tqdm(total=total, desc="Scraping", unit="pages", bar_format="{desc}: {n_fmt}/{total_fmt} {bar} {rate_fmt}")

        successful, failed = [], []

        # Create actors
        fetchers = [AsyncAiohttpFetcher.remote(images_path=self.images_file, timeout=60, max_retries=3)
                   for _ in range(self.cur_actors)]

        # Submit all URLs as individual tasks
        futures = []
        for i, url in enumerate(self.urls):
            actor = fetchers[i % self.cur_actors]
            futures.append(actor.fetch_single.remote(url))

        # Process results as they complete
        remaining_futures = futures[:]
        recent_results = []  # Track recent results for adaptive control

        while remaining_futures:
            # Wait for at least one task to complete
            ready, remaining_futures = ray.wait(remaining_futures, num_returns=min(10, len(remaining_futures)))

            # Process completed tasks
            batch_success, batch_failed = [], []
            for future in ready:
                result = ray.get(future)
                recent_results.append(result)

                if "error" in result:
                    batch_failed.append(result)
                    failed.append(result)
                else:
                    batch_success.append(result)
                    successful.append(result)

                progress.update(1)  # Update progress bar for each completed URL

            # Adaptive control - check every 50 completed requests
            if len(recent_results) >= 50:
                error_rate = len([r for r in recent_results if "error" in r]) / len(recent_results)

                if error_rate > self.throttle_threshold and self.cur_actors > self.min_actors:
                    self.cur_actors -= 1
                    print(f"\nHigh error rate ({error_rate:.2%}). Reducing concurrency to {self.cur_actors}")
                    # Recreate actors with new count
                    fetchers = [AsyncAiohttpFetcher.remote(images_path=self.images_file, timeout=60, max_retries=3)
                               for _ in range(self.cur_actors)]
                    await asyncio.sleep(2)

                elif error_rate < 0.05 and self.cur_actors < self.max_actors:
                    self.cur_actors += 1
                    print(f"\nError rate low ({error_rate:.2%}). Increasing concurrency to {self.cur_actors}")
                    # Recreate actors with new count
                    fetchers = [AsyncAiohttpFetcher.remote(images_path=self.images_file, timeout=60, max_retries=3)
                               for _ in range(self.cur_actors)]

                recent_results = []  # Reset for next batch of monitoring

        # Stream results to files
        self._stream_to_jsonl(self.output_path, successful)
        self._stream_to_jsonl(self.error_path, failed)

        progress.close()
        print("\n----- Sitemap scraping summary -----")
        print(f"Scraped {len(successful)} pages → {self.output_path}")
        print(f"Scraped images → {self.images_file}")
        print(f"Failed {len(failed)} pages → {self.error_path}")
        print("------------------------------------\n")
