import json
from collections import Counter

def process_urls(json_file_path="Mamaearth_crawled_urls.json"):
    """
    Reads a JSON file with an array of URLs, processes them,
    and saves unique and duplicate URLs to separate files.
    """
    all_urls = []
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            all_urls = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{json_file_path}' was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_file_path}'. Please ensure it's valid JSON.")
        return

    # Convert all URLs to lowercase
    all_urls_lower = [url.lower() for url in all_urls]

    # Use Counter to find duplicates
    url_counts = Counter(all_urls_lower)

    unique_urls = []
    duplicate_urls = []

    for url in all_urls_lower:
        if url_counts[url] == 1:
            unique_urls.append(url)
        else:
            duplicate_urls.append(url)

    # Convert to sets to ensure true uniqueness for output files if needed,
    # though the logic above puts all instances of duplicates into duplicate_urls
    # If you only want one instance of each duplicate URL, use set() on duplicate_urls
    unique_urls_set = sorted(list(set(unique_urls)))
    duplicate_urls_set = sorted(list(set([url for url, count in url_counts.items() if count > 1])))

    if len(unique_urls_set) > 0:
        # Save unique URLs to a file
        unique_output_file = "unique_urls.json"
        with open(unique_output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_urls_set, f, indent=2, ensure_ascii=False)
        print(f"Unique URLs saved to '{unique_output_file}'")

    if len(duplicate_urls_set) > 0:
        # Save duplicate URLs to a file
        # This will save only one instance of each duplicated URL that was found
        duplicates_output_file = "duplicates.json"
        with open(duplicates_output_file, 'w', encoding='utf-8') as f:
            json.dump(duplicate_urls_set, f, indent=2, ensure_ascii=False)
        print(f"Duplicate URLs saved to '{duplicates_output_file}'")

    # Summary
    total_links_original = len(all_urls)
    total_unique_found = len(unique_urls_set)
    total_duplicates_count = total_links_original - total_unique_found # This is the count of individual duplicate entries

    print("\n--- Processing Summary ---")
    print(f"Total links in original file: {total_links_original}")
    print(f"Total unique links found (after lowercasing): {total_unique_found}")
    print(f"Amount of duplicate entries found: {total_duplicates_count}") # This counts each occurrence of a duplicate
    print(f"Number of distinct URLs that are duplicated: {len(duplicate_urls_set)}")


if __name__ == "__main__":
    import sys
    file_path = sys.argv[1]
    process_urls(file_path)
