import json
import ray
from tqdm import tqdm

API_KEY = "AIzaSyBffOv-K3BdTpo8kIEbFdL0OSlZLDpMFhw"

# Set lower CPU usage per task to allow higher parallelism
@ray.remote(num_cpus=0.5)
def detect_text_with_language_detection_ray(api_key, image_url):
    import requests

    vision_url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
    payload = {
        "requests": [
            {
                "image": {"source": {"imageUri": image_url}},
                "features": [{"type": "DOCUMENT_TEXT_DETECTION"}]
            }
        ]
    }

    try:
        response = requests.post(
            vision_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Error: {response.status_code} - {response.text}",
                "data": {"total": 0}
            }

        data = response.json()
        full_text = data['responses'][0].get('fullTextAnnotation', {}).get('text', '')

        language_word_counts = {}
        total_word_count = 0
        document_language = 'en'

        if ('property' in data['responses'][0].get('fullTextAnnotation', {}) and
            'detectedLanguages' in data['responses'][0]['fullTextAnnotation']['property']):
            document_language = data['responses'][0]['fullTextAnnotation']['property']['detectedLanguages'][0]['languageCode']

        for page in data['responses'][0].get('fullTextAnnotation', {}).get('pages', []):
            for block in page.get('blocks', []):
                block_language = document_language
                if 'property' in block and 'detectedLanguages' in block['property']:
                    block_language = block['property']['detectedLanguages'][0]['languageCode']

                for paragraph in block.get('paragraphs', []):
                    paragraph_language = block_language
                    if 'property' in paragraph and 'detectedLanguages' in paragraph['property']:
                        paragraph_language = paragraph['property']['detectedLanguages'][0]['languageCode']

                    for word in paragraph.get('words', []):
                        total_word_count += 1
                        language_code = paragraph_language
                        if 'property' in word and 'detectedLanguages' in word['property']:
                            language_code = word['property']['detectedLanguages'][0]['languageCode']

                        language_word_counts[language_code] = language_word_counts.get(language_code, 0) + 1

        if total_word_count == 0:
            words = full_text.split()
            total_word_count = len(words)
            language_word_counts[document_language] = total_word_count

        return {
            "success": True,
            "error": None,
            "data": {
                "languages": language_word_counts,
                "total": total_word_count
            }
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {"total": 0}
        }


def run_image_analysis(input_file: str, output_filename: str) -> dict:
    language_fullname_map = {
        "en": "English",
        "hi": "Hindi",
        "bn": "Bengali",
        "te": "Telugu",
        "ta": "Tamil",
        "gu": "Gujarati",
        "mr": "Marathi",
        "kn": "Kannada",
        "ml": "Malayalam",
        "or": "Oriya",
        "pa": "Punjabi",
        "as": "Assamese",
        "others": "Others"
    }

    indian_languages = {
        "hi", "bn", "te", "ta", "gu", "mr", "kn", "ml", "or", "pa", "as"
    }

    output_keys = list(indian_languages) + ["en"]
    word_counts = {language_fullname_map[lang]: 0 for lang in output_keys}
    word_counts["others"] = 0
    word_counts["total"] = 0

    # Load unique image URLs
    url_set = set()
    with open(input_file, "r", encoding="utf-8") as reader:
        for line in reader:
            url = line.strip().strip('"')
            if url:
                url_set.add(url)

    # Submit tasks to Ray
    futures = [
        detect_text_with_language_detection_ray.remote(API_KEY, image_url)
        for image_url in url_set
    ]

    # Process all results in parallel with live progress
    results = []
    remaining = list(futures)

    with tqdm(total=len(futures), desc="Analyzing Images", unit="img") as pbar:
        while remaining:
            done, remaining = ray.wait(remaining, num_returns=min(8, len(remaining)), timeout=1.0)
            for obj_ref in done:
                result = ray.get(obj_ref)
                results.append(result)
                pbar.update(1)

    # Process results
    for result in results:
        if result["success"]:
            total = result["data"]["total"]
            word_counts["total"] += total
            for lang, count in result["data"]["languages"].items():
                if lang in language_fullname_map:
                    word_counts[language_fullname_map[lang]] += count
                else:
                    word_counts["others"] += count

    # Compute percentages
    percentages = {
        lang: round((count / word_counts["total"]) * 100, 2) if word_counts["total"] > 0 else 0.0
        for lang, count in word_counts.items() if lang != "total"
    }

    # Prepare final result
    result = {
        "word_counts": word_counts,
        "percentages": percentages
    }

    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("\n----- Image analysis completed -----")
    print(f"Summary and Language analysis of images saved at -> {output_filename}")
    print("------------------------------\n")

    return result


if __name__ == "__main__":
    import sys

    input_file = sys.argv[1] if len(sys.argv) > 1 else "outputs/wordwise_images_urls.jsonl"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "outputs/wordwise_image_analysis.json"

    ray.init(ignore_reinit_error=True)

    run_image_analysis(input_file, output_file)
