import json
import re
from langdetect import detect, DetectorFactory
from collections import defaultdict, OrderedDict

DetectorFactory.seed = 42  # Ensure consistent language detection

INDIAN_LANGS = {"hi", "bn", "ta", "te", "mr", "gu", "pa", "kn", "ml", "or", "as", "en"}

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def tokenize(text):
    return re.findall(r'\b\w+\b', text)

def detect_language_chunks(text, chunk_size=50):
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

def process_langdetect_to_json(input_path, output_path):
    results = []
    total_lang_counts = defaultdict(int)
    total_words = 0

    with open(input_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = json.loads(line)
                url = obj.get("url")
                text = clean_text(obj.get("data", ""))

                if not text:
                    continue

                lang_word_counts = detect_language_chunks(text)
                lang_word_counts = {k: v for k, v in lang_word_counts.items() if v >= 5}
                page_total = sum(lang_word_counts.values())

                if not lang_word_counts or page_total == 0:
                    continue

                # Update global summary counters
                for lang, count in lang_word_counts.items():
                    total_lang_counts[lang] += count
                total_words += page_total

                percentages = {
                    lang: round((count / page_total) * 100, 2)
                    for lang, count in lang_word_counts.items()
                }

                result = {
                    "url": url,
                    "languages_used": list(lang_word_counts.keys()),
                    "word_count_per_language": lang_word_counts,
                    "percentage_per_language": percentages,
                    "total_counted_words": page_total
                }

                results.append(result)

            except Exception:
                continue

    # Calculate overall summary percentages
    summary_percentages = {
        lang: round((count / total_words) * 100, 2)
        for lang, count in total_lang_counts.items()
    }

    summary = {
        "summary": {
            "total_words_per_language": dict(total_lang_counts),
            "percentage_per_language": summary_percentages,
            "total_words_counted": total_words
        },
        "pages": results
    }

    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump(summary, outfile, indent=2, ensure_ascii=False)

# Example usage:
process_langdetect_to_json("output.jsonl", "language_analysis.json")
