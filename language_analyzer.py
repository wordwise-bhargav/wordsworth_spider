import ray
import asyncio
import json
import re
import os
from pathlib import Path
from collections import Counter
from typing import List, Dict, Tuple
from tqdm import tqdm
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 42  # for consistent results

LANGUAGES = {
    "en": "English",
    "hi": "Hindi",
    "bn": "Bengali",
    "te": "Telugu",
    "mr": "Marathi",
    "ta": "Tamil",
    "ur": "Urdu",
    "gu": "Gujarati",
    "kn": "Kannada",
    "ml": "Malayalam",
    "or": "Odia",
    "pa": "Punjabi",
    "as": "Assamese"
}

def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def split_into_chunks(text: str, chunk_size: int = 300) -> List[str]:
    words = text.split()
    return [' '.join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]

def detect_language_chunks(text: str) -> Dict[str, int]:
    chunks = split_into_chunks(text)
    lang_counts = Counter()

    for chunk in chunks:
        try:
            lang = detect(chunk)
            if lang in LANGUAGES:
                lang_counts[LANGUAGES[lang]] += len(chunk.split())
        except Exception:
            continue

    return dict(lang_counts)


@ray.remote
class LanguageAnalyzer:
    def analyze_text(self, obj: dict) -> Tuple[dict, dict]:
        url = obj.get("url")
        text = clean_text(obj.get("content", ""))

        if not text:
            return {}, {}

        lang_word_counts = detect_language_chunks(text)
        total = sum(lang_word_counts.values())
        if total == 0:
            return {}, {}

        lang_percentages = {lang: round((count / total) * 100, 2) for lang, count in lang_word_counts.items()}

        result = {
            "url": url,
            "total_words": total,
            "languages": lang_word_counts,
            "percentages": lang_percentages
        }

        return result, lang_word_counts


async def run_language_analysis(
    input_path="outputs/site_data.jsonl",
    output_path="outputs/language_analysis.json"
):
    num_cpus = os.cpu_count() or 2
    num_actors = num_cpus
    actors = [LanguageAnalyzer.remote() for _ in range(num_actors)]

    input_file = Path(input_path)
    output_file = Path(output_path)
    output_file.unlink(missing_ok=True)

    with input_file.open("r", encoding="utf-8") as infile:
        lines = [json.loads(line.strip()) for line in infile if line.strip()]

    total_urls = len(lines)
    progress = tqdm(total=total_urls, desc="Analyzing Languages", unit="pages")

    futures = []
    for i, line in enumerate(lines):
        actor = actors[i % num_actors]
        futures.append(actor.analyze_text.remote(line))

    resolved = ray.get(futures)

    overall_counts = Counter()
    total_words = 0
    page_results = []

    for result, counts in resolved:
        if result:
            page_results.append(result)
            overall_counts.update(counts)
            total_words += result["total_words"]
        progress.update(1)

    progress.close()

    summary = {
        "total_pages": total_urls,
        "total_words": total_words,
        "overall_language_counts": dict(overall_counts),
        "overall_percentages": {
            lang: round((count / total_words) * 100, 2)
            for lang, count in overall_counts.items()
        }
    }

    final_output = {
        "pages": page_results,
        "summary": summary
    }

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)

    print(f"\nSummary and Language analysis saved at -> {output_path}")

if __name__ == "__main__":
    asyncio.run(run_language_analysis(
        "outputs/wordwise_site_data.jsonl",
        "outputs/wordwise_language_analysis.json"))
