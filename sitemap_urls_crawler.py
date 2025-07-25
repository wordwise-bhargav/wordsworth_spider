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
    def __init__(self, timeout: int = 10, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> dict:
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(url) as response:
                    text = await response.text()
                    soup = BeautifulSoup(text, "lxml")
                    content = soup.get_text(separator="\n", strip=True)
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
        error_file: str = "scrape_errors.jsonl",
        batch_size: int = 20,
        max_actors: int = 8,
        min_actors: int = 1,
        throttle_threshold: float = 0.15  # If >15% fail, reduce concurrency
    ):
        self.urls = urls
        self.output_path = Path(output_file)
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

        batches = self._split_batches()
        total = len(self.urls)
        progress = tqdm(total=total, desc="Scraping", unit="pages", bar_format="{desc}: {n_fmt}/{total_fmt} {bar} {rate_fmt}")

        successful, failed = [], []
        batch_index = 0

        while batch_index < len(batches):
            current_batch_group = batches[batch_index:batch_index + self.cur_actors]
            fetchers = [AsyncAiohttpFetcher.remote(timeout=60, max_retries=3) for _ in range(len(current_batch_group))]
            tasks = []

            for i, batch in enumerate(current_batch_group):
                actor = fetchers[i]
                tasks.append(actor.fetch.remote(batch))

            results_batches = ray.get(tasks)

            temp_success, temp_failed = [], []
            for batch_result in results_batches:
                for item in batch_result:
                    if "error" in item:
                        temp_failed.append(item)
                    else:
                        temp_success.append(item)

            successful.extend(temp_success)
            failed.extend(temp_failed)
            progress.update(len(temp_success) + len(temp_failed))

            # Adaptive control
            error_rate = len(temp_failed) / max(len(temp_success) + len(temp_failed), 1)
            if error_rate > self.throttle_threshold and self.cur_actors > self.min_actors:
                self.cur_actors -= 1
                print(f"High error rate ({error_rate:.2%}). Reducing concurrency to {self.cur_actors} and throttling...")
                await asyncio.sleep(5)
            elif error_rate < 0.05 and self.cur_actors < self.max_actors:
                self.cur_actors += 1
                print(f"Error rate low. Increasing concurrency to {self.cur_actors}")

            batch_index += len(current_batch_group)

        self._stream_to_jsonl(self.output_path, successful)
        self._stream_to_jsonl(self.error_path, failed)

        progress.close()
        print(f"\nScraped {len(successful)} pages → {self.output_path}")
        print(f"Failed {len(failed)} pages → {self.error_path}")
