import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import csv
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field, fields as dataclass_fields
from typing import List, Dict, Optional, Tuple, Set, Any
import traceback

# --- Data Class (Modified) ---
@dataclass
class Record:
    url: str
    company_name: Optional[str] = None
    phone_numbers: Set[str] = field(default_factory=set)
    social_media_links: Set[str] = field(default_factory=set)
    addresses: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict:
        return {f.name: getattr(self, f.name) for f in dataclass_fields(self)}

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [f.name for f in dataclass_fields(cls)]

    def merge_with(self, other_record: 'Record'):
        self.social_media_links = self.social_media_links | other_record.social_media_links
        self.phone_numbers = self.phone_numbers | other_record.phone_numbers
        self.addresses = self.addresses | other_record.addresses


# --- WebFetcher Class (Simplified for single page fetch) ---
class WebFetcher:
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    REQUEST_TIMEOUT_SEC = 15  
    
    def __init__(self, user_agent: Optional[str] = None, request_timeout: Optional[int] = None):
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.request_timeout = request_timeout or self.REQUEST_TIMEOUT_SEC
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {'User-Agent': self.user_agent}
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            self._session = aiohttp.ClientSession(
                headers=headers, timeout=timeout
            )
        return self._session

    async def close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_single_page_async(self, url: str) -> Optional[bytes]:
        """Fetches a single URL, returns html content"""
        session = await self._get_session()
        try:
            async with session.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                content_type_header = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type_header:
                    # TODO: Log errors
                    return None
                html_content = await response.read()
        except aiohttp.ClientError as e:
            return None#, f"Fetch ClientError for {url}: {type(e).__name__} - {e}"
        except asyncio.TimeoutError:
            return None#, f"Fetch Timeout for {url}"
        except Exception as e:
            return None#, f"Unexpected fetch error for {url}: {type(e).__name__} - {e}"

        return html_content
        # TODO: Soup from html content in extractor, not here
        if html_content is None:
            return None, None#, f"No content from {url}"

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()
            page_text = ' '.join(soup.stripped_strings)
            return soup, page_text, None
        except Exception as e:
            return None, None, f"HTML parsing error for {url}: {e}"


# --- HTML Data Extractor Class (Modified to include contact page discovery) ---
class HtmlDataExtractor:
    PHONE_REGEX = re.compile(r'''
        (\+?\d{1,3}[\s.-]?)? (\(?\d{2,5}\)?[\s.-]?)? (\d{2,5}[\s.-]?){1,2} \d{3,5} (?!\d)
    ''', re.VERBOSE)
    SOCIAL_MEDIA_PATTERNS = {
        "facebook": re.compile(r"facebook\.com/([a-zA-Z0-9._-]+/?)(?!.*\b(?:sharer|plugins|events|groups|notes|photo)\b)"),
        "twitter": re.compile(r"(?:twitter|x)\.com/([a-zA-Z0-9_]{1,15})(?!\b(?:intent|share|search)\b)"),
        "linkedin": re.compile(r"linkedin\.com/(company/|in/)([a-zA-Z0-9._-]+/?)(?!.*\bshare\b)"),
        "instagram": re.compile(r"instagram\.com/([a-zA-Z0-9._]+/?)(?!.*\b(?:p/|explore)\b)"),
        "youtube": re.compile(r"youtube\.com/(user/|channel/|c/)?([a-zA-Z0-9._-]+/?)(?!.*\b(?:watch|embed|results|playlist)\b)")
    }
    ADDRESS_KEYWORDS = ['address', 'location', 'contact', 'office', 'headquarters']
    ADDRESS_MIN_LENGTH = 10
    ADDRESS_MAX_LENGTH = 200
    BASIC_ADDRESS_PART_REGEX = re.compile(r'\d+\s+[A-Za-z]+\s+(Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Boulevard|Blvd)\b', re.IGNORECASE)
    CONTACT_PAGE_KEYWORDS = [
        'contact', 'kontakt', 'contact-us', 'contactus', 
        'get-in-touch', 'reach-us', 'support', 'help', 'impressum', 'about' # 'about' can sometimes lead to contact
    ]
    CONTACT_LINK_TEXT_PATTERNS = [
        re.compile(r'\bcontact\b', re.IGNORECASE), re.compile(r'\bkontakt\b', re.IGNORECASE),
        re.compile(r'contact us', re.IGNORECASE), re.compile(r'get in touch', re.IGNORECASE),
        re.compile(r'support', re.IGNORECASE), re.compile(r'help', re.IGNORECASE),
        re.compile(r'impressum', re.IGNORECASE), re.compile(r'\babout us\b', re.IGNORECASE)
    ]

    @staticmethod
    def _normalize_url(url: str, base_url: str) -> str: # Keep this utility
        parsed_url = urlparse(urljoin(base_url, url.strip()))
        scheme = parsed_url.scheme if parsed_url.scheme else urlparse(base_url).scheme
        if not scheme or scheme.lower() not in ['http', 'https']: scheme = 'http'
        # TODO: check if strip is needed + minimum dependencies from here; maybe urlparse().geturl() instead of everything here
        return f"{scheme}://{parsed_url.netloc}{parsed_url.path}".strip('/')
        
    @staticmethod
    def _clean_phone_number(number_str: str) -> str:
        return re.sub(r'[\s().-]', '', number_str)

    def _extract_phone_numbers(self, soup: BeautifulSoup, page_text: str) -> List[str]:
        phones = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('tel:'):
                # TODO: Check if re.sub from below can be used here, or directly in clean phone number function; also check if library can handle normalization
                phones.add(self._clean_phone_number(href.replace('tel:', '').strip()))
        for match_groups in self.PHONE_REGEX.findall(page_text):
            full_match = "".join(filter(None, match_groups))
            if len(re.sub(r'\D', '', full_match)) >= 7:
                phones.add(self._clean_phone_number(full_match))
        return phones

    def _extract_social_media_links(self, soup: BeautifulSoup, base_url: str) -> set[str]:
        # TODO: check why social media links are not extracted from page text
        extracted_social_links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if not href or href.startswith(('#', 'mailto:', 'javascript:')): continue
            absolute_url = self._normalize_url(href, base_url)
            parsed_absolute_url = urlparse(absolute_url)
            for platform_key, pattern in self.SOCIAL_MEDIA_PATTERNS.items():
                match = pattern.search(absolute_url)
                if match:
                    clean_link = absolute_url
                    if platform_key == "twitter":
                        profile_part = match.group(1); domain_matched = parsed_absolute_url.netloc
                        clean_link = f"https://{domain_matched}/{profile_part.strip('/')}"
                    elif platform_key == "facebook": profile_part = match.group(1); clean_link = f"https://www.facebook.com/{profile_part.strip('/')}"
                    elif platform_key == "linkedin": profile_part = match.group(2); clean_link = f"https://www.linkedin.com/{match.group(1).strip('/')}/{profile_part.strip('/')}"
                    elif platform_key == "instagram": profile_part = match.group(1); clean_link = f"https://www.instagram.com/{profile_part.strip('/')}"
                    elif platform_key == "youtube": profile_part = match.group(2); prefix = match.group(1) if match.group(1) else "user/"; clean_link = f"https://www.youtube.com/{prefix.strip('/')}/{profile_part.strip('/')}"
                    extracted_social_links.add(clean_link)
                    break
                    
        return extracted_social_links

    def _extract_addresses(self, soup: BeautifulSoup, page_text: str) -> List[str]:
        addresses = set()
        text_nodes = soup.find_all(string=True)
        potential_address_texts = []
        for text_node in text_nodes:
            parent = text_node.parent
            if parent.name in ['script', 'style', 'head', 'title', 'meta', '[document]', 'noscript']: continue
            text_content = text_node.strip()
            if self.ADDRESS_MIN_LENGTH < len(text_content) < self.ADDRESS_MAX_LENGTH:
                for keyword in self.ADDRESS_KEYWORDS:
                    if keyword in text_content.lower():
                        potential_address_texts.append(text_content)
                        
                if self.BASIC_ADDRESS_PART_REGEX.search(text_content):
                    potential_address_texts.append(text_content)
        
        for text_segment in page_text.splitlines(): 
            if self.ADDRESS_MIN_LENGTH < len(text_segment) < self.ADDRESS_MAX_LENGTH:
                for keyword in self.ADDRESS_KEYWORDS:
                    if keyword in text_segment.lower():
                        addresses.add(text_segment.strip())
                        
                if self.BASIC_ADDRESS_PART_REGEX.search(text_segment):
                    addresses.add(text_segment.strip())

        for address_tag in soup.find_all(['address', lambda tag: tag.has_attr('itemprop') and tag['itemprop'] == 'address']):
            address_text = ' '.join(address_tag.get_text(separator=' ', strip=True).split())
            if len(address_text) > self.ADDRESS_MIN_LENGTH: 
                addresses.add(address_text)
        
        for text in potential_address_texts: 
            # TODO: check if regex can be used
            if self.BASIC_ADDRESS_PART_REGEX.search(text) and any(char.isdigit() for char in text):
                addresses.add(text)
        return addresses


    def extract_all_data(self, current_page_url: str, html_content: str) -> Record:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()
            page_text = ' '.join(soup.stripped_strings)
            
            phone_numbers = self._extract_phone_numbers(soup, page_text)
            social_media_links = self._extract_social_media_links(soup, current_page_url)
            addresses = self._extract_addresses(soup, page_text)
            
            # This record is temporary - its URL will be overwritten or used for merging.
            return Record( 
                url=current_page_url, # Temporary, will be used for matching or logging in Orchestrator
                phone_numbers=phone_numbers,
                addresses=addresses,
                social_media_links=social_media_links
            )
        except Exception as e:
            # import traceback; traceback.print_exc()
            # TODO: log error or treat in orchestrator
            return Record()
            return Record.create_error_record(current_page_url, f"Extraction error: {e}")


# --- Orchestrator Class (Significantly Modified) ---
class Orchestrator:
    CONCURRENT_REQUESTS = 100
    
    def __init__(self, input_csv_file: str, output_data_file: str, concurrent_requests: Optional[int] = None):
        self.input_csv_file = input_csv_file
        self.output_data_file = output_data_file
        self.web_fetcher = WebFetcher()
        self.html_extractor = HtmlDataExtractor()
        self.concurrent_requests = concurrent_requests or self.CONCURRENT_REQUESTS

    def _prepare_url(self, url_input: str) -> str:
        url_input = url_input.strip()
        if not url_input: return ""
        if not re.match(r'^[a-zA-Z]+://', url_input):
            return 'http://' + url_input
        return url_input

    async def _fetch_and_extract_initial(self, original_url: str, semaphore: asyncio.Semaphore) -> Tuple[str, Optional[Record]]:
        """Fetches main page, extracts data, and discovers contact URL."""
        async with semaphore:
            html_content = await self.web_fetcher.fetch_single_page_async(original_url)

            if not html_content:
                # TODO: Log err here
                return original_url, None

            record_main = self.html_extractor.extract_all_data(original_url, html_content)
            record_main.url = original_url # Ensure record's URL is the primary one

            return original_url, record_main

    async def _run_async_core(self):
        # --- Phase 0: Read CSV ---
        initial_urls_from_csv: List[str] = []
        final_records: Dict[str, List[Record]] = {}

        print(f"Reading websites from: {self.input_csv_file}")
        try:
            with open(self.input_csv_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if "domain" not in (reader.fieldnames or []):
                    # TODO: Log error
                    return
                for row in reader:
                    raw_url = row.get("domain", "").strip()
                    if not raw_url:
                        # TODO: Log empty URL warning
                        continue
                    prepared_url = self._prepare_url(raw_url)
                    initial_urls_from_csv.append(prepared_url)
        except FileNotFoundError:
             # TODO: Log file not found error
            return
        except Exception as e:
            # TODO: Log unexpected error
            traceback.print_exc(); return

        if not initial_urls_from_csv:
            # TODO: Log no valid URLs found
            return

        semaphore = asyncio.Semaphore(self.concurrent_requests)

        # --- Phase 1: Initial Fetch and Contact URL Discovery ---
        print(f"\n--- Round 1: Processing {len(initial_urls_from_csv)} initial URLs ---")
        round1_tasks = [
            self._fetch_and_extract_initial(url, semaphore)
            for url in initial_urls_from_csv
        ]

        if round1_tasks:
            round1_results = await asyncio.gather(*round1_tasks, return_exceptions=True)
            for i, res in enumerate(round1_results):
                original_url = initial_urls_from_csv[i]
                
                try:
                    _ , record_main = res
                except TypeError:
                    record_main = Record(original_url)
                
                final_records[original_url] = record_main
        
        print(f"Round 1 complete. Processed {len(final_records)} records.")
        
        # --- Phase 2: Contact Page Fetching ---
        # TODO: Log discovered contact pages
        
        # --- Phase 3: Display and Save Results ---
        print("\n--- Processing Complete. Finalizing Results ---")
        scraped_results_list: List[Record] = []
        # Use initial_urls_from_csv to maintain order and include all attempted URLs
        # Add any error URLs from CSV parsing (like empty rows) that weren't in initial_urls_from_csv


        for original_url in initial_urls_from_csv:
            record_instance = final_records.get(original_url)
            if not record_instance:
                # TODO: Log missing record error
                record_instance = Record(original_url)
            
            scraped_results_list.append(record_instance) # For saving
        
        self._save_results(scraped_results_list)


    async def run_async_with_cleanup(self):
        try:
            await self._run_async_core()
        except Exception as e: 
            print(f"A critical error occurred in the async orchestrator: {e}")
            import traceback; traceback.print_exc()
        finally:
            if hasattr(self.web_fetcher, 'close_session'):
                await self.web_fetcher.close_session()

    def run(self): 
        try:
            asyncio.run(self.run_async_with_cleanup())
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Shutting down.")

    def _save_results(self, results_to_save: List[Record]):
        """Saves the results to a CSV file."""
        fieldnames = Record.get_field_names()
        try:
            with open(self.output_data_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                for record_obj in results_to_save:
                    row_data_dict = record_obj.to_dict()
                    csv_ready_row = {}
                    for key, value in row_data_dict.items():
                        if isinstance(value, set): # Handles processing_notes as well
                             csv_ready_row[key] = '; '.join(map(str,value)) # Ensure all list items are strings
                        elif value is None: 
                             csv_ready_row[key] = ''
                        else:
                            csv_ready_row[key] = str(value)
                    writer.writerow(csv_ready_row)
            print(f"Successfully saved data for {len(results_to_save)} entries to {self.output_data_file}")
        except IOError as e:
            # TODO: Log file write error
            pass
        except Exception as e:
            import traceback; traceback.print_exc()


# --- Main Execution ---
if __name__ == "__main__":
    input_file = 'sample-websites.csv' 

    output_file = 'scraped_company_data_two_rounds_new.csv'
    
    orchestrator = Orchestrator(input_csv_file=input_file, 
                                output_data_file=output_file,
                                concurrent_requests=100) # Concurrency for each round
    orchestrator.run()