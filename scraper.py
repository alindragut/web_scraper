import cProfile
import requests
from bs4 import BeautifulSoup
import re
import csv
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field, fields as dataclass_fields
from typing import List, Dict, Optional, Tuple

# --- Data Class ---
@dataclass
class Record:
    """
    A pure data class to hold scraped information for a URL,
    with individual fields for each social media platform.
    """
    url: str
    phone_numbers: List[str] = field(default_factory=list)
    facebook_links: List[str] = field(default_factory=list)
    twitter_x_links: List[str] = field(default_factory=list) # For Twitter and X.com
    linkedin_links: List[str] = field(default_factory=list)
    instagram_links: List[str] = field(default_factory=list)
    youtube_links: List[str] = field(default_factory=list)
    addresses: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        """Converts the Record object to a dictionary."""
        return {f.name: getattr(self, f.name) for f in dataclass_fields(self)}

    @classmethod
    def get_field_names(cls) -> List[str]:
        """Returns a list of field names for the Record class."""
        return [f.name for f in dataclass_fields(cls)]

    @classmethod
    def create_error_record(cls, url: str, error_message: str) -> 'Record':
        """Creates a Record instance populated only with URL and error."""
        # Dataclass default_factory will handle empty lists for other fields
        return cls(url=url, error=error_message)


# --- HTML Data Extractor Class ---
class HtmlDataExtractor:
    """
    Extracts specific data (phones, social media, addresses) from HTML content.
    This class now owns the regex patterns and extraction logic.
    """
    # --- Configuration & Regular Expressions ---
    PHONE_REGEX = re.compile(r'''
        (\+?\d{1,3}[\s.-]?)?        # Optional country code
        (\(?\d{2,5}\)?[\s.-]?)?     # Optional area code (2-5 digits)
        (\d{2,5}[\s.-]?){1,2}       # First part of local number
        \d{3,5}                     # Last part of local number
        (?!\d)                      # Not followed by another digit
    ''', re.VERBOSE)

    # Updated Twitter regex to include x.com
    # The key "twitter" will now map to twitter_x_links in Record
    SOCIAL_MEDIA_PATTERNS = {
        "facebook": re.compile(r"facebook\.com/([a-zA-Z0-9._-]+/?)(?!.*\b(?:sharer|plugins|events|groups|notes|photo)\b)"),
        "twitter": re.compile(r"(?:twitter|x)\.com/([a-zA-Z0-9_]{1,15})(?!\b(?:intent|share|search)\b)"),
        "linkedin": re.compile(r"linkedin\.com/(company/|in/)([a-zA-Z0-9._-]+/?)(?!.*\bshare\b)"),
        "instagram": re.compile(r"instagram\.com/([a-zA-Z0-9._]+/?)(?!.*\b(?:p/|explore)\b)"),
        "youtube": re.compile(r"youtube\.com/(user/|channel/|c/)?([a-zA-Z0-9._-]+/?)(?!.*\b(?:watch|embed|results|playlist)\b)")
    }

    # Mapping from SOCIAL_MEDIA_PATTERNS keys to Record field names
    SOCIAL_MEDIA_FIELD_MAP = {
        "facebook": "facebook_links",
        "twitter": "twitter_x_links",
        "linkedin": "linkedin_links",
        "instagram": "instagram_links",
        "youtube": "youtube_links",
    }

    ADDRESS_KEYWORDS = ['address', 'location', 'contact', 'office', 'headquarters']
    BASIC_ADDRESS_PART_REGEX = re.compile(r'\d+\s+[A-Za-z]+\s+(Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Boulevard|Blvd)\b', re.IGNORECASE)

    @staticmethod
    def _normalize_url(url: str, base_url: str) -> str:
        parsed_url = urlparse(urljoin(base_url, url.strip()))
        scheme = parsed_url.scheme if parsed_url.scheme else urlparse(base_url).scheme
        if not scheme or scheme.lower() not in ['http', 'https']: scheme = 'http'
        return f"{scheme}://{parsed_url.netloc}{parsed_url.path}".strip('/')

    @staticmethod
    def _clean_phone_number(number_str: str) -> str:
        return re.sub(r'[\s().-]', '', number_str)

    def _extract_phone_numbers(self, soup: BeautifulSoup, page_text: str) -> List[str]:
        phones = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('tel:'):
                phones.add(self._clean_phone_number(href.replace('tel:', '').strip()))
        for match_groups in self.PHONE_REGEX.findall(page_text):
            full_match = "".join(filter(None, match_groups))
            if len(re.sub(r'\D', '', full_match)) >= 7:
                phones.add(self._clean_phone_number(full_match))
        return list(phones)

    def _extract_social_media_links_individual(self, soup: BeautifulSoup, base_url: str) -> Dict[str, List[str]]:
        # This will return a dictionary with keys like "facebook_links", "twitter_x_links", etc.
        extracted_social_links = {field_name: set() for field_name in self.SOCIAL_MEDIA_FIELD_MAP.values()}

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if not href or href.startswith(('#', 'mailto:', 'javascript:')): continue
            
            absolute_url = self._normalize_url(href, base_url)
            parsed_absolute_url = urlparse(absolute_url) # For domain extraction

            for platform_key, pattern in self.SOCIAL_MEDIA_PATTERNS.items():
                match = pattern.search(absolute_url)
                if match:
                    record_field_name = self.SOCIAL_MEDIA_FIELD_MAP[platform_key]
                    clean_link = absolute_url # Default
                    
                    if platform_key == "twitter": # Handles both twitter.com and x.com
                        profile_part = match.group(1)
                        # Use the domain that was actually matched (twitter.com or x.com)
                        domain_matched = parsed_absolute_url.netloc 
                        clean_link = f"https://{domain_matched}/{profile_part.strip('/')}"
                    elif platform_key == "facebook":
                        profile_part = match.group(1)
                        clean_link = f"https://www.facebook.com/{profile_part.strip('/')}"
                    elif platform_key == "linkedin":
                        profile_part = match.group(2)
                        clean_link = f"https://www.linkedin.com/{match.group(1).strip('/')}/{profile_part.strip('/')}"
                    elif platform_key == "instagram":
                        profile_part = match.group(1)
                        clean_link = f"https://www.instagram.com/{profile_part.strip('/')}"
                    elif platform_key == "youtube":
                        profile_part = match.group(2)
                        prefix = match.group(1) if match.group(1) else "user/"
                        clean_link = f"https://www.youtube.com/{prefix.strip('/')}/{profile_part.strip('/')}"
                    
                    extracted_social_links[record_field_name].add(clean_link)
                    break 
        
        return {field_name: list(links) for field_name, links in extracted_social_links.items()}

    def _extract_addresses(self, soup: BeautifulSoup, page_text: str) -> List[str]:
        addresses = set()
        text_nodes = soup.find_all(string=True)
        potential_address_texts = []
        for t_node in text_nodes:
            parent = t_node.parent
            if parent.name in ['script', 'style', 'head', 'title', 'meta', '[document]', 'noscript']: continue
            text_content = t_node.strip()
            if 10 < len(text_content) < 200:
                if any(keyword in text_content.lower() for keyword in self.ADDRESS_KEYWORDS) or \
                   self.BASIC_ADDRESS_PART_REGEX.search(text_content):
                    potential_address_texts.append(text_content)
        for address_tag in soup.find_all(['address', lambda tag: tag.has_attr('itemprop') and tag['itemprop'] == 'address']):
            address_text = ' '.join(address_tag.get_text(separator=' ', strip=True).split())
            if len(address_text) > 10: addresses.add(address_text)
        for text in potential_address_texts:
            if self.BASIC_ADDRESS_PART_REGEX.search(text) and any(char.isdigit() for char in text):
                addresses.add(text)
        return list(addresses)

    def extract_all_data(self, url: str, soup: BeautifulSoup, page_text: str) -> Record:
        try:
            phones = self._extract_phone_numbers(soup, page_text)
            social_media_dict = self._extract_social_media_links_individual(soup, url)
            addresses = self._extract_addresses(soup, page_text)
            
            # Unpack social_media_dict into Record constructor
            return Record(
                url=url,
                phone_numbers=phones,
                addresses=addresses,
                error=None,
                **social_media_dict # This maps "facebook_links": [...] to the Record field
            )
        except Exception as e:
            print(f"Error during data extraction by HtmlDataExtractor for {url}: {e}")
            # import traceback; traceback.print_exc() # For debugging
            return Record.create_error_record(url, f"Extraction error: {e}")


# --- Web Fetcher Class ---
class WebFetcher:
    """
    Responsible for fetching and performing initial parsing of web page content.
    """
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'

    def __init__(self, user_agent: Optional[str] = None, request_timeout: int = 15):
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.request_timeout = request_timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})

    def fetch_page_content(self, url: str) -> Tuple[Optional[BeautifulSoup], Optional[str], Optional[str]]:
        """ Fetches page, returns (soup, page_text, error_message_or_None) """
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            return None, None, str(e)
        
        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' not in content_type:
            return None, None, f"Skipping {url}, not HTML content (type: {content_type})"
        
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            return None, None, f"Error parsing HTML for {url}: {e}"
        
        for script_or_style in soup(["script", "style"]):
            script_or_style.decompose()
        page_text = ' '.join(soup.stripped_strings)
        return soup, page_text, None

# --- Orchestrator Class ---
class Orchestrator:
    """
    Manages the end-to-end workflow: reading URLs, fetching, extracting, and saving data.
    """
    def __init__(self, input_csv_file: str, output_data_file: str):
        self.input_csv_file = input_csv_file
        self.output_data_file = output_data_file
        self.web_fetcher = WebFetcher()
        self.html_extractor = HtmlDataExtractor()
        self.scraped_results: List[Dict] = []

    def _prepare_url(self, url_input: str) -> str:
        url_input = url_input.strip()
        if not url_input: return ""
        if not re.match(r'^[a-zA-Z]+://', url_input):
            return 'http://' + url_input
        return url_input

    def process_single_url(self, url_to_scrape: str) -> Record:
        print(f"Processing: {url_to_scrape}")
        soup, page_text, fetch_error = self.web_fetcher.fetch_page_content(url_to_scrape)

        if fetch_error:
            print(f"Fetch/Parse error for {url_to_scrape}: {fetch_error}")
            return Record.create_error_record(url_to_scrape, fetch_error)
        
        if soup is None or page_text is None: # Should be covered by fetch_error
            error_msg = "Failed to retrieve or parse page content correctly (soup or text is None)."
            print(f"Content error for {url_to_scrape}: {error_msg}")
            return Record.create_error_record(url_to_scrape, error_msg)

        return self.html_extractor.extract_all_data(url_to_scrape, soup, page_text)

    def run(self):
        print(f"Reading websites from: {self.input_csv_file}")
        try:
            with open(self.input_csv_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if "domain" not in reader.fieldnames:
                    print(f"Error: CSV file '{self.input_csv_file}' must contain a 'domain' column.")
                    return

                for row_num, row in enumerate(reader, 1):
                    raw_url = row.get("domain", "")
                    url_to_scrape = self._prepare_url(raw_url)
                    if not url_to_scrape:
                        print(f"Skipping empty URL in CSV at row {row_num}.")
                        continue
                    
                    record_instance = self.process_single_url(url_to_scrape)
                    self.scraped_results.append(record_instance.to_dict())
                    
                    # Console Output
                    data_dict = record_instance.to_dict()
                    error_msg = f"Error: {data_dict['error']}" if data_dict['error'] else "OK"
                    social_summary = []
                    for f_name in Record.get_field_names():
                        if "_links" in f_name and f_name != "social_media_links": # general field no longer exists
                           social_summary.append(f"{f_name.replace('_links','').capitalize()}: {len(data_dict.get(f_name,[]))}")
                    
                    print(f"Data for {data_dict['url']}: Phones: {len(data_dict.get('phone_numbers',[]))}, "
                          f"Social: {{ {', '.join(social_summary)} }}, "
                          f"Addresses: {len(data_dict.get('addresses',[]))}, Status: {error_msg}")
                    print("-" * 30)

        except FileNotFoundError:
            print(f"Error: Input file '{self.input_csv_file}' not found.")
            return
        except Exception as e:
            print(f"An unexpected error occurred during CSV processing or main loop: {e}")
            import traceback; traceback.print_exc()
            return

        self._save_results()

    def _save_results(self):
        print(f"\nSaving all scraped data to {self.output_data_file}...")
        if not self.scraped_results:
            print("No data was successfully scraped or collected.")
            return

        fieldnames = Record.get_field_names() # Get field names directly from Record class
        try:
            with open(self.output_data_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                for row_data_dict in self.scraped_results:
                    # Prepare for CSV: join lists into strings
                    csv_ready_row = {}
                    for key, value in row_data_dict.items():
                        if isinstance(value, list):
                            csv_ready_row[key] = '; '.join(value)
                        elif value is None: # Ensure None errors are empty strings
                             csv_ready_row[key] = ''
                        else:
                            csv_ready_row[key] = str(value)
                    writer.writerow(csv_ready_row)
            print(f"Successfully saved data for {len(self.scraped_results)} websites to {self.output_data_file}")
        except IOError as e:
            print(f"Error writing to output file '{self.output_data_file}': {e}")
        except Exception as e:
            print(f"An unexpected error occurred during CSV writing: {e}")
            import traceback; traceback.print_exc()


# --- Main Execution ---
if __name__ == "__main__":
    input_file = 'sample-websites.csv' 
    output_file = 'scraped_company_data_new.csv' # New version
    
    orchestrator = Orchestrator(input_csv_file=input_file, output_data_file=output_file)
    orchestrator.run()