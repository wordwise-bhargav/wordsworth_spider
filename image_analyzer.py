import json
import requests
from tqdm import tqdm

# API Key
API_KEY = "<GOOGLE_CLOUD_VISION_API_KEY>"

# Detects text from a URL and counts words per language using improved language detection.
def detect_text_with_language_detection(api_key, image_url, show_fulltext=False):
    vision_url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
    payload = {
        "requests": [
            {
                "image": {"source": {"imageUri": image_url}},
                "features": [{"type": "DOCUMENT_TEXT_DETECTION"}]
            }
        ]
    }
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

    try:
        full_text = data['responses'][0]['fullTextAnnotation']['text']
        if show_fulltext:
            print("Detected Text:")
            print(full_text)

        language_word_counts = {}
        total_word_count = 0
        document_language = 'en'
        if ('property' in data['responses'][0]['fullTextAnnotation'] and
            'detectedLanguages' in data['responses'][0]['fullTextAnnotation']['property']):
            document_language = data['responses'][0]['fullTextAnnotation']['property']['detectedLanguages'][0]['languageCode']

        for page in data['responses'][0]['fullTextAnnotation']['pages']:
            for block in page['blocks']:
                block_language = document_language
                if 'property' in block and 'detectedLanguages' in block['property']:
                    block_language = block['property']['detectedLanguages'][0]['languageCode']

                for paragraph in block['paragraphs']:
                    paragraph_language = block_language
                    if 'property' in paragraph and 'detectedLanguages' in paragraph['property']:
                        paragraph_language = paragraph['property']['detectedLanguages'][0]['languageCode']

                    for word in paragraph['words']:
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

    except (KeyError, IndexError) as e:
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

    # ✅ Read plain text lines from file
    url_set = set()
    with open(input_file, "r", encoding="utf-8") as reader:
        for line in reader:
            url = line.strip().strip('"')
            if url:
                url_set.add(url)

    # Analyze each image with progress bar
    progress = tqdm(url_set, desc="Analyzing Images", unit="img")
    for image_url in progress:
        result = detect_text_with_language_detection(API_KEY, image_url)
        if result["success"]:
            total = result["data"]["total"]
            word_counts["total"] += total
            for lang, count in result["data"]["languages"].items():
                if lang in language_fullname_map:
                    word_counts[language_fullname_map[lang]] += count
                else:
                    word_counts["others"] += count
        else:
            progress.set_postfix(error="yes")

        progress.set_postfix(total_words=word_counts["total"])

    # Compute percentages (excluding "total")
    percentages = {
        lang: round((count / word_counts["total"]) * 100, 2)
        for lang, count in word_counts.items() if lang != "total"
    }

    # Prepare final result
    result = {
        "word_counts": word_counts,
        "percentages": percentages
    }

    # Save to file
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("\n----- Image analysis completed -----")
    print(f"Summary and Language analysis of images saved at -> {output_filename}")
    print("------------------------------\n")

    return result

if __name__ == "__main__":
    result = run_image_analysis("outputs/wordwise_images_urls.jsonl", "outputs/wordwise_image_analysis.json")
    print(json.dumps(result, indent=2, ensure_ascii=False))
