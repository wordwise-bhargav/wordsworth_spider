import ray
import json
import re
from langdetect import detect, DetectorFactory
from collections import defaultdict
from typing import Dict, List

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
    def analyze_text(self, obj: dict) -> dict:
        url = obj.get("url")
        text = clean_text(obj.get("data", ""))

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

class ParallelLangDetectionProcessor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def run(self):
        ray.init(num_cpus=None)
        analyzer = LanguageAnalyzer.remote()

        with open(self.input_path, "r", encoding="utf-8") as f:
            lines = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    lines.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # Skip bad lines


        futures = [analyzer.analyze_text.remote(obj) for obj in lines] # type: ignore
        results = ray.get(futures)

        summary, pages = self._build_summary(results)

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump({
                "summary": summary,
                "pages": pages
            }, f, indent=2, ensure_ascii=False)

        ray.shutdown()

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

if __name__ == "__main__":
    processor = ParallelLangDetectionProcessor("output.jsonl", "language_analysis.json")
    processor.run()
