from typing import Tuple
import ray
import json
import re
from langdetect import detect, DetectorFactory
from collections import defaultdict
from typing import Dict, List
import threading

# Ensure consistent language detection
DetectorFactory.seed = 42

# Accepted Indian languages (Currently set to 12)
INDIAN_LANGS = {"hi", "bn", "ta", "te", "mr", "gu", "pa", "kn", "ml", "or", "as", "en"}

def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()

def tokenize(text: str) -> List[str]:
    return re.findall(r'\b\w+\b', text)

def detect_language_chunks(text: str, chunk_size: int = 50) -> Dict[str, int]:
    words = tokenize(text)
    lang_counts = defaultdict(int)

    for i in range(0, len(words), chunk_size):
        chunk = ' '.join(words[i:i + chunk_size])
        try:
            lang = detect(chunk)
            if lang in INDIAN_LANGS:
                lang_counts[lang] += len(chunk.split())
        except:
            continue

    return lang_counts

@ray.remote
class LanguageAnalyzer:
    def analyze_text(self, obj: Tuple[str, str]) -> dict:
        url, raw_content = obj
        text = clean_text(raw_content)

        if not text:
            return {}

        lang_word_counts = detect_language_chunks(text)
        lang_word_counts = {k: v for k, v in lang_word_counts.items() if v >= 5}
        page_total = sum(lang_word_counts.values())

        if not lang_word_counts or page_total == 0:
            return {}

        percentages = {
            lang: round((count / page_total) * 100, 2)
            for lang, count in lang_word_counts.items()
        }

        return {
            "url": url,
            "languages_used": list(lang_word_counts.keys()),
            "word_count_per_language": lang_word_counts,
            "percentage_per_language": percentages,
            "total_counted_words": page_total
        }

class ParallelLangDetectionProcessorQueue:
    def __init__(self, queue, output_path="language_analysis.json"):
        self.queue = queue
        self.output_path = output_path
        self.analyzer = LanguageAnalyzer.remote()
        self.results = []
        self.result_refs = []
        self._stop = False

    def consume_queue(self):
        """Continuously consume from queue and send to Ray actor."""
        with open("analysis_log.txt", "a", encoding="utf-8") as log_file:
            while not self._stop or not self.queue.empty():
                try:
                    obj = self.queue.get(timeout=1)
                    url, _ = obj
                except Exception:
                    continue
                try:
                    log_file.write(f"Started analysis for {url}\n")
                    log_file.flush()

                    ref = self.analyzer.analyze_text.remote(obj)
                    self.result_refs.append(ref)
                except Exception:
                    log_file.write(f"Analysis failed for {url}\n")
                    log_file.flush()
                    continue  # Timeout, keep looping

    def run(self):
        consumer_thread = threading.Thread(target=self.consume_queue)
        consumer_thread.start()

        consumer_thread.join()

        # Gather results
        results = ray.get(self.result_refs)
        results = [r for r in results if r]  # remove empty ones

        print(f"\nWriting the output analysis to {self.output_path}")

        # Generate summary and write it to file at output_path
        summary, pages = self._build_summary(results)
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump({
                "summary": summary,
                "pages": pages
            }, f, indent=2, ensure_ascii=False)

    def _build_summary(self, results: List[dict]):
        total_lang_counts = defaultdict(int)
        total_words = 0
        pages = []

        for res in results:
            if not res:
                continue
            pages.append(res)
            for lang, count in res["word_count_per_language"].items():
                total_lang_counts[lang] += count
            total_words += res["total_counted_words"]

        summary_percentages = {
            lang: round((count / total_words) * 100, 2)
            for lang, count in total_lang_counts.items()
        }

        summary = {
            "total_words_per_language": dict(total_lang_counts),
            "percentage_per_language": summary_percentages,
            "total_words_counted": total_words
        }

        return summary, pages

    def stop(self):
        self._stop = True
