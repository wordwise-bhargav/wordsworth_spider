import re
from langdetect import detect, DetectorFactory
from collections import defaultdict
from typing import Dict, List, Tuple

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

def analyze_text(obj: Tuple[str, str]) -> dict:
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
