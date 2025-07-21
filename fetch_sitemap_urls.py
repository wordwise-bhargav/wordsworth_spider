import re
import gzip
import time
import logging
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin
from typing import Set, Tuple, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SitemapAnalyzer:
    def __init__(self, url="https://www.wordwise.one/", max_workers=5, timeout=10):
        self.base_url = self._normalize_url(url)
        self.allowed_domains = self._setup_allowed_domains()
        self.discovered_urls = set()
        self.sitemaps_to_process = set()
        self.max_workers = max_workers
        self.timeout = timeout

        # Session for connection pooling and better performance
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; SitemapAnalyzer/1.0)'
        })

        # Comprehensive file extensions to exclude
        self.file_extensions = {
            # Images
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico', '.tiff', '.tif',
            # Documents
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf', '.odt',
            # Archives
            '.zip', '.rar', '.tar', '.gz', '.7z', '.bz2', '.xz',
            # Media
            '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ogg', '.wav', '.m4a',
            # Code/Data files
            '.css', '.js', '.xml', '.json', '.csv', '.sql', '.log',
            # Fonts
            '.ttf', '.otf', '.woff', '.woff2', '.eot',
            # Other
            '.exe', '.dmg', '.deb', '.rpm', '.msi', '.apk'
        }

        # Common sitemap locations (reduced list for efficiency)
        self.common_sitemaps = [
            'sitemap.xml', 'sitemap_index.xml', 'sitemap-index.xml',
            'wp-sitemap.xml', 'sitemap.xml.gz', 'sitemap_index.xml.gz'
        ]

    def _setup_allowed_domains(self) -> Set[str]:
        """Setup allowed domains including www and non-www variants"""
        parsed_base_domain = urlparse(self.base_url).netloc.lower()
        domains = {parsed_base_domain}

        if parsed_base_domain.startswith('www.'):
            domains.add(parsed_base_domain[4:])
        else:
            domains.add(f'www.{parsed_base_domain}')

        return domains

    def _normalize_url(self, url: str) -> str:
        """Normalize URL"""
        if not url.startswith(('http://', 'https://')):
            url = "https://" + url

        parsed_url = urlparse(url)
        clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        if parsed_url.query:
            clean_url += "?" + parsed_url.query

        # Remove trailing slash except for root
        if clean_url.endswith('/') and len(parsed_url.path) > 1:
            clean_url = clean_url.rstrip('/')

        return clean_url

    def _is_allowed_url(self, url: str) -> bool:
        """Check if URL belongs to allowed domains"""
        try:
            parsed_url = urlparse(url)
            return parsed_url.netloc.lower() in self.allowed_domains
        except Exception:
            return False

    def _is_webpage_url(self, url: str) -> bool:
        """Check for webpage URLs, excluding assets and files"""
        try:
            parsed_url = urlparse(url)
            path = parsed_url.path.lower()

            # Check file extensions
            if any(path.endswith(ext) for ext in self.file_extensions):
                return False

            # Check for asset directories
            asset_patterns = [
                r'/wp-content/uploads/',
                r'/assets/',
                r'/static/',
                r'/media/',
                r'/images/',
                r'/img/',
                r'/css/',
                r'/js/',
                r'/fonts/',
                r'/downloads/',
                r'/files/',
                r'\.min\.',  # minified files
            ]

            if any(re.search(pattern, path) for pattern in asset_patterns):
                return False

            # Additional checks for query parameters that suggest files
            if parsed_url.query:
                query_lower = parsed_url.query.lower()
                if any(param in query_lower for param in ['download', 'file', 'attachment']):
                    return False

            return True

        except Exception:
            return False

    def _get_sitemap_urls_from_robots(self) -> List[str]:
        """Extract sitemap URLs from robots.txt"""
        sitemap_urls = []
        robots_url = urljoin(self.base_url, 'robots.txt')

        try:
            response = self.session.get(robots_url, timeout=self.timeout)
            if response.status_code == 200:
                for line in response.text.splitlines():
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line[len('sitemap:'):].strip()
                        if self._is_allowed_url(sitemap_url):
                            sitemap_urls.append(sitemap_url)
        except requests.exceptions.RequestException as e:
            logger.debug(f"Could not fetch robots.txt: {e}")

        return sitemap_urls

    def _find_sitemap_url(self) -> Optional[str]:
        """Find sitemap URL with improved efficiency"""
        # First check robots.txt
        sitemap_urls = self._get_sitemap_urls_from_robots()

        # Add common sitemap locations
        for sitemap_path in self.common_sitemaps:
            sitemap_urls.append(urljoin(self.base_url, sitemap_path))

        # Test URLs concurrently
        with ThreadPoolExecutor(max_workers=min(5, len(sitemap_urls))) as executor:
            future_to_url = {
                executor.submit(self._test_sitemap_url, url): url
                for url in sitemap_urls
            }

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    if future.result():
                        return url
                except Exception as e:
                    logger.debug(f"Error testing {url}: {e}")

        return None

    def _test_sitemap_url(self, url: str) -> bool:
        """Test if a sitemap URL exists and is valid"""
        try:
            response = self.session.head(url, timeout=self.timeout)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').lower()
                return any(ct in content_type for ct in ['xml', 'gzip', 'application/octet-stream'])
        except requests.exceptions.RequestException:
            pass
        return False

    def _fetch_sitemap(self, sitemap_url: str) -> Optional[bytes]:
        """Fetch sitemap with better error handling and gzip support"""
        try:
            response = self.session.get(sitemap_url, timeout=self.timeout)
            response.raise_for_status()

            # Handle gzipped content
            content_type = response.headers.get('Content-Type', '').lower()
            if 'gzip' in content_type or sitemap_url.endswith('.gz'):
                try:
                    return gzip.decompress(response.content)
                except gzip.BadGzipFile:
                    return response.content

            return response.content

        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching sitemap {sitemap_url}: {e}")
            return None

    def _parse_sitemap(self, sitemap_content: bytes) -> Tuple[Set[str], Set[str]]:
        """Parse sitemap with improved error handling"""
        urls = set()
        sitemap_index_urls = set()

        try:
            # Try to parse as XML
            root = ET.fromstring(sitemap_content)

            # Handle different namespace variations
            namespaces = {
                'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                '': 'http://www.sitemaps.org/schemas/sitemap/0.9'
            }

            # Check if it's a sitemap index
            if 'sitemapindex' in root.tag.lower():
                for sitemap_elem in root.findall('.//sitemap', namespaces):
                    loc_elem = sitemap_elem.find('loc', namespaces)
                    if loc_elem is not None and loc_elem.text:
                        url = loc_elem.text.strip()
                        if self._is_allowed_url(url):
                            sitemap_index_urls.add(url)
            else:
                # Regular sitemap
                for url_elem in root.findall('.//url', namespaces):
                    loc_elem = url_elem.find('loc', namespaces)
                    if loc_elem is not None and loc_elem.text:
                        url = loc_elem.text.strip()
                        if (self._is_allowed_url(url) and
                            self._is_webpage_url(url)):
                            urls.add(url)

        except ET.ParseError as e:
            logger.warning(f"Error parsing sitemap XML: {e}")
            # Try to extract URLs with regex as fallback
            urls.update(self._extract_urls_with_regex(sitemap_content))

        except Exception as e:
            logger.warning(f"Unexpected error parsing sitemap: {e}")

        return urls, sitemap_index_urls

    def _extract_urls_with_regex(self, content: bytes) -> Set[str]:
        """Fallback URL extraction using regex"""
        urls = set()
        try:
            text_content = content.decode('utf-8', errors='ignore')
            url_pattern = r'<loc>(https?://[^<]+)</loc>'
            matches = re.findall(url_pattern, text_content, re.IGNORECASE)

            for url in matches:
                url = url.strip()
                if (self._is_allowed_url(url) and
                    self._is_webpage_url(url)):
                    urls.add(url)

        except Exception as e:
            logger.debug(f"Regex extraction failed: {e}")

        return urls

    def _process_sitemap(self, sitemap_url: str) -> Tuple[Set[str], Set[str]]:
        """Process a single sitemap"""
        logger.info(f"Processing sitemap: {sitemap_url}")

        sitemap_content = self._fetch_sitemap(sitemap_url)
        if sitemap_content:
            return self._parse_sitemap(sitemap_content)

        return set(), set()

    def get_all_urls(self) -> Tuple[Set[str], bool]:
        """Get all webpage URLs from sitemaps with concurrent processing"""
        initial_sitemap_url = self._find_sitemap_url()
        if not initial_sitemap_url:
            logger.warning("Could not find a sitemap for the given URL.")
            return set(), False

        self.sitemaps_to_process.add(initial_sitemap_url)
        processed_sitemaps = set()

        while self.sitemaps_to_process:
            current_batch = list(self.sitemaps_to_process - processed_sitemaps)
            if not current_batch:
                break

            # Process sitemaps concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_sitemap = {
                    executor.submit(self._process_sitemap, sitemap_url): sitemap_url
                    for sitemap_url in current_batch
                }

                for future in as_completed(future_to_sitemap):
                    sitemap_url = future_to_sitemap[future]
                    try:
                        urls, sitemap_index_urls = future.result()

                        # Add discovered URLs
                        self.discovered_urls.update(urls)

                        # Add new sitemaps to process
                        for new_sitemap_url in sitemap_index_urls:
                            if new_sitemap_url not in processed_sitemaps:
                                self.sitemaps_to_process.add(new_sitemap_url)

                    except Exception as e:
                        logger.error(f"Error processing sitemap {sitemap_url}: {e}")

                    processed_sitemaps.add(sitemap_url)

            # Remove processed sitemaps
            self.sitemaps_to_process -= processed_sitemaps

        logger.info(f"Discovered {len(self.discovered_urls)} webpage URLs")
        return self.discovered_urls, True

    def close(self):
        """Close the session"""
        if hasattr(self, 'session'):
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
