import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import csv
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field, fields as dataclass_fields
from typing import List, Dict, Optional, Tuple, Set, Any

# --- Data Class (Modified) ---
@dataclass
class Record:
    url: str # This will be the primary URL from the CSV
    phone_numbers: List[str] = field(default_factory=list)
    facebook_links: List[str] = field(default_factory=list)
    twitter_x_links: List[str] = field(default_factory=list)
    linkedin_links: List[str] = field(default_factory=list)
    instagram_links: List[str] = field(default_factory=list)
    youtube_links: List[str] = field(default_factory=list)
    addresses: List[str] = field(default_factory=list)
    error: Optional[str] = None
    processing_notes: List[str] = field(default_factory=list) # For additional info

    def to_dict(self) -> Dict:
        return {f.name: getattr(self, f.name) for f in dataclass_fields(self)}

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [f.name for f in dataclass_fields(cls)]

    @classmethod
    def create_error_record(cls, url: str, error_message: str) -> 'Record':
        return cls(url=url, error=error_message)

    def add_processing_note(self, note: str):
        self.processing_notes.append(note)
        # Optionally, prepend to main error or handle differently
        # if self.error:
        # self.error += f"; Note: {note}"
        # else:
        # self.error = f"Note: {note}"


    def merge_with(self, other_record: 'Record', source_page_url: str):
        """Merges data from another record (e.g., from a contact page)."""
        self.add_processing_note(f"Attempting to merge data from: {source_page_url}")
        if other_record.error:
            self.add_processing_note(f"Source page {source_page_url} had error: {other_record.error}")
            # Decide if other_record.error should affect self.error
            # For now, just log it as a note.

        # Combine and deduplicate list fields
        list_fields_to_merge = [
            'phone_numbers', 'facebook_links', 'twitter_x_links', 
            'linkedin_links', 'instagram_links', 'youtube_links', 'addresses'
        ]
        for field_name in list_fields_to_merge:
            self_list: List[str] = getattr(self, field_name)
            other_list: List[str] = getattr(other_record, field_name)
            
            # Simple extend and set for deduplication. Could be more sophisticated.
            combined = self_list + [item for item in other_list if item not in self_list]
            setattr(self, field_name, combined)
        
        self.add_processing_note(f"Merge from {source_page_url} complete.")


# --- WebFetcher Class (Simplified for single page fetch) ---
class WebFetcher:
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

    def __init__(self, user_agent: Optional[str] = None, request_timeout: int = 15):
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.request_timeout = request_timeout
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

    async def fetch_single_page_async(self, url: str) -> Tuple[Optional[BeautifulSoup], Optional[str], Optional[str]]:
        """Fetches a single URL, returns (soup, text, error_msg)"""
        session = await self._get_session()
        html_content: Optional[bytes] = None
        try:
            # print(f"DEBUG WebFetcher: Fetching {url}")
            async with session.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                content_type_header = response.headers.get('Content-Type', '').lower()
                actual_url = str(response.url) # Get final URL after redirects
                if 'text/html' not in content_type_header:
                    return None, None, f"Skipping non-HTML {url} (final URL: {actual_url}, type: {content_type_header})"
                html_content = await response.read()
        except aiohttp.ClientError as e:
            return None, None, f"Fetch ClientError for {url}: {type(e).__name__} - {e}"
        except asyncio.TimeoutError:
            return None, None, f"Fetch Timeout for {url}"
        except Exception as e:
            return None, None, f"Unexpected fetch error for {url}: {type(e).__name__} - {e}"

        if html_content is None:
            return None, None, f"No content from {url}"

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
    # ... (Regexes and other methods remain largely the same) ...
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
    SOCIAL_MEDIA_FIELD_MAP = {
        "facebook": "facebook_links", "twitter": "twitter_x_links",
        "linkedin": "linkedin_links", "instagram": "instagram_links",
        "youtube": "youtube_links",
    }
    ADDRESS_KEYWORDS = ['address', 'location', 'contact', 'office', 'headquarters']
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
        return f"{scheme}://{parsed_url.netloc}{parsed_url.path}".strip('/')
        
    def discover_contact_page_url(self, main_page_soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Scans the main page soup for contact page links. (Moved from WebFetcher)"""
        main_domain = urlparse(base_url).netloc
        potential_links: List[Tuple[str, int]] = [] 

        for a_tag in main_page_soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            link_text = a_tag.get_text(separator=' ', strip=True).lower()

            if not href or href.startswith(('#', 'mailto:', 'javascript:', 'tel:')):
                continue

            abs_url = self._normalize_url(href, base_url) # Use internal normalize
            parsed_abs_url = urlparse(abs_url)

            if parsed_abs_url.netloc != main_domain:
                continue
            
            score = 0
            for pattern in self.CONTACT_LINK_TEXT_PATTERNS:
                if pattern.search(link_text):
                    score += 5
                    break
            path_lower = parsed_abs_url.path.lower()
            for keyword in self.CONTACT_PAGE_KEYWORDS:
                if f'/{keyword}' in path_lower or f'{keyword}/' in path_lower or keyword == path_lower.strip('/'):
                    score += 2
                    break
            
            if score > 0:
                is_present = False
                for i, (existing_url, existing_score) in enumerate(potential_links):
                    if existing_url == abs_url:
                        is_present = True
                        if score > existing_score: potential_links[i] = (abs_url, score)
                        break
                if not is_present: potential_links.append((abs_url, score))
        
        # Fallback for common paths if no links found with good score
        if not potential_links or max(p[1] for p in potential_links) < 3 : # Arbitrary threshold
            common_paths_to_try = ['contact', 'contact-us', 'kontakt', 'impressum', 'about/contact']
            for path_segment in common_paths_to_try:
                # Construct carefully to avoid double slashes if base_url already has a path
                parsed_base = urlparse(base_url)
                # Ensure base_url ends with a slash if it's just a domain, or use its path
                base_path_for_join = f"{parsed_base.scheme}://{parsed_base.netloc}"
                if parsed_base.path and parsed_base.path != '/':
                     # If base_url has a path, join relative to that.
                     # However, contact pages are often relative to root.
                     # Let's prioritize root for these common fallbacks.
                     pass # Keep base_path_for_join as root for now
                
                test_url = urljoin(f"{parsed_base.scheme}://{parsed_base.netloc}/", path_segment) # Join from root
                
                # Add if not already effectively present
                if not any(pl[0] == test_url for pl in potential_links):
                    potential_links.append((test_url, 1)) # Low score for guessed paths

        if not potential_links: return None
        potential_links.sort(key=lambda x: (-x[1], len(x[0])))
        
        # print(f"DEBUG HtmlExtractor: Potential contact links for {base_url}: {potential_links}")
        if potential_links:
            chosen_url = potential_links[0][0]
            # print(f"DEBUG HtmlExtractor: Chosen contact link for {base_url}: {chosen_url}")
            # Avoid choosing the base_url itself if it somehow got a score
            if chosen_url == self._normalize_url(base_url, base_url): # Normalize base_url for comparison
                if len(potential_links) > 1 and potential_links[1][0] != chosen_url:
                    return potential_links[1][0]
                else:
                    return None # Only found itself
            return chosen_url
        return None

    # ... _extract_phone_numbers, _extract_social_media_links_individual, _extract_addresses ...
    # These methods now operate on the soup/text of a *single* page.
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
        extracted_social_links = {field_name: set() for field_name in self.SOCIAL_MEDIA_FIELD_MAP.values()}
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if not href or href.startswith(('#', 'mailto:', 'javascript:')): continue
            absolute_url = self._normalize_url(href, base_url)
            parsed_absolute_url = urlparse(absolute_url)
            for platform_key, pattern in self.SOCIAL_MEDIA_PATTERNS.items():
                match = pattern.search(absolute_url)
                if match:
                    record_field_name = self.SOCIAL_MEDIA_FIELD_MAP[platform_key]
                    clean_link = absolute_url
                    if platform_key == "twitter":
                        profile_part = match.group(1); domain_matched = parsed_absolute_url.netloc
                        clean_link = f"https://{domain_matched}/{profile_part.strip('/')}"
                    elif platform_key == "facebook": profile_part = match.group(1); clean_link = f"https://www.facebook.com/{profile_part.strip('/')}"
                    elif platform_key == "linkedin": profile_part = match.group(2); clean_link = f"https://www.linkedin.com/{match.group(1).strip('/')}/{profile_part.strip('/')}"
                    elif platform_key == "instagram": profile_part = match.group(1); clean_link = f"https://www.instagram.com/{profile_part.strip('/')}"
                    elif platform_key == "youtube": profile_part = match.group(2); prefix = match.group(1) if match.group(1) else "user/"; clean_link = f"https://www.youtube.com/{prefix.strip('/')}/{profile_part.strip('/')}"
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
        
        for text_segment in page_text.splitlines(): 
            if 10 < len(text_segment) < 200:
                 if any(keyword in text_segment.lower() for keyword in self.ADDRESS_KEYWORDS) or \
                   self.BASIC_ADDRESS_PART_REGEX.search(text_segment):
                    if any(char.isdigit() for char in text_segment):
                        addresses.add(text_segment.strip())

        for address_tag in soup.find_all(['address', lambda tag: tag.has_attr('itemprop') and tag['itemprop'] == 'address']):
            address_text = ' '.join(address_tag.get_text(separator=' ', strip=True).split())
            if len(address_text) > 10: addresses.add(address_text)
        
        for text in potential_address_texts: 
            if self.BASIC_ADDRESS_PART_REGEX.search(text) and any(char.isdigit() for char in text):
                addresses.add(text)
        return list(addresses)


    def extract_all_data(self, current_page_url: str, soup: BeautifulSoup, page_text: str) -> Record:
        # current_page_url is the URL of the page *being processed*, not necessarily the original CSV URL.
        # The Record's main 'url' field will be set by the Orchestrator to the original CSV URL.
        try:
            phones = self._extract_phone_numbers(soup, page_text)
            social_media_dict = self._extract_social_media_links_individual(soup, current_page_url)
            addresses = self._extract_addresses(soup, page_text)
            
            # This record is temporary; its URL will be overwritten or used for merging.
            return Record( 
                url=current_page_url, # Temporary, will be used for matching or logging in Orchestrator
                phone_numbers=phones,
                addresses=addresses,
                error=None,
                **social_media_dict
            )
        except Exception as e:
            # import traceback; traceback.print_exc()
            return Record.create_error_record(current_page_url, f"Extraction error: {e}")


# --- Orchestrator Class (Significantly Modified) ---
class Orchestrator:
    def __init__(self, input_csv_file: str, output_data_file: str, concurrent_requests: int = 10):
        self.input_csv_file = input_csv_file
        self.output_data_file = output_data_file
        self.web_fetcher = WebFetcher(request_timeout=20) # Increased timeout slightly
        self.html_extractor = HtmlDataExtractor()
        # self.scraped_results: List[Dict] = [] # Will be populated at the end from final_records
        self.concurrent_requests = concurrent_requests

    def _prepare_url(self, url_input: str) -> str:
        url_input = url_input.strip()
        if not url_input: return ""
        if not re.match(r'^[a-zA-Z]+://', url_input):
            return 'http://' + url_input
        return url_input

    async def _fetch_and_extract_initial(self, original_url: str, semaphore: asyncio.Semaphore) -> Tuple[str, Optional[Record], Optional[str]]:
        """Fetches main page, extracts data, and discovers contact URL."""
        async with semaphore:
            # print(f"Orchestrator Round 1: Processing {original_url}")
            main_soup, main_text, fetch_error = await self.web_fetcher.fetch_single_page_async(original_url)

            if fetch_error or not main_soup or main_text is None:
                err_msg = fetch_error or "Main page content/soup missing"
                # print(f"Orchestrator Round 1: Error for {original_url} - {err_msg}")
                return original_url, Record.create_error_record(original_url, err_msg), None

            record_main = self.html_extractor.extract_all_data(original_url, main_soup, main_text)
            record_main.url = original_url # Ensure record's URL is the primary one

            contact_url: Optional[str] = None
            if not record_main.error: # Only try to find contact if main page processing was okay
                try:
                    contact_url = self.html_extractor.discover_contact_page_url(main_soup, original_url)
                    if contact_url == original_url: # Don't re-fetch the same page
                        contact_url = None
                    if contact_url:
                        record_main.add_processing_note(f"Discovered potential contact page: {contact_url}")
                except Exception as e:
                    record_main.add_processing_note(f"Error discovering contact page for {original_url}: {e}")
            # print(f"Orchestrator Round 1: Success for {original_url}. Contact URL: {contact_url}")
            return original_url, record_main, contact_url

    async def _fetch_and_extract_contact(self, original_url_ref: str, contact_page_url: str, semaphore: asyncio.Semaphore) -> Tuple[str, str, Optional[Record]]:
        """Fetches contact page and extracts its data."""
        async with semaphore:
            # print(f"Orchestrator Round 2: Processing contact {contact_page_url} (for {original_url_ref})")
            contact_soup, contact_text, fetch_error = await self.web_fetcher.fetch_single_page_async(contact_page_url)

            if fetch_error or not contact_soup or contact_text is None:
                err_msg = fetch_error or "Contact page content/soup missing"
                # print(f"Orchestrator Round 2: Error for {contact_page_url} - {err_msg}")
                # Create a minimal error record for the contact page to pass the error message
                error_contact_record = Record.create_error_record(contact_page_url, err_msg)
                return original_url_ref, contact_page_url, error_contact_record

            record_contact_page_data = self.html_extractor.extract_all_data(contact_page_url, contact_soup, contact_text)
            # record_contact_page_data.url is contact_page_url here from extractor
            # print(f"Orchestrator Round 2: Success for {contact_page_url}")
            return original_url_ref, contact_page_url, record_contact_page_data


    async def _run_async_core(self):
        # --- Phase 0: Read CSV ---
        initial_urls_from_csv: List[Tuple[int, str]] = [] # (row_num, url)
        # final_records will store the master Record for each original_url
        final_records: Dict[str, Record] = {} # Keyed by original_url

        print(f"Reading websites from: {self.input_csv_file}")
        try:
            with open(self.input_csv_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if "domain" not in (reader.fieldnames or []):
                    print(f"Error: CSV file '{self.input_csv_file}' must contain a 'domain' column.")
                    return
                for row_num, row in enumerate(reader, 1):
                    raw_url = row.get("domain", "").strip()
                    if not raw_url:
                        err_url_key = f"CSV_Row_{row_num}_Empty"
                        final_records[err_url_key] = Record.create_error_record(err_url_key, "Empty URL in CSV")
                        print(f"Skipping empty URL in CSV at row {row_num}. Logged as error.")
                        continue
                    prepared_url = self._prepare_url(raw_url)
                    initial_urls_from_csv.append((row_num, prepared_url))
        except FileNotFoundError:
            print(f"Error: Input file '{self.input_csv_file}' not found.")
            return
        except Exception as e:
            print(f"An unexpected error occurred during CSV reading: {e}")
            import traceback; traceback.print_exc(); return

        if not initial_urls_from_csv and not final_records:
            print("No valid URLs found in the CSV to process.")
            self._save_results(list(final_records.values()))
            return

        semaphore = asyncio.Semaphore(self.concurrent_requests)

        # --- Phase 1: Initial Fetch and Contact URL Discovery ---
        print(f"\n--- Round 1: Processing {len(initial_urls_from_csv)} initial URLs ---")
        round1_tasks = [
            self._fetch_and_extract_initial(url, semaphore)
            for _, url in initial_urls_from_csv
        ]
        
        contact_pages_to_fetch: List[Tuple[str, str]] = [] # (original_url, contact_page_url)

        if round1_tasks:
            round1_results = await asyncio.gather(*round1_tasks, return_exceptions=True)
            for i, res_or_exc in enumerate(round1_results):
                original_row_num, original_url = initial_urls_from_csv[i]
                if isinstance(res_or_exc, Exception):
                    print(f"CRITICAL Orchestration exception (Round 1) for {original_url} (row {original_row_num}): {res_or_exc}")
                    final_records[original_url] = Record.create_error_record(original_url, f"Task exception R1: {res_or_exc}")
                else:
                    _, record_main, discovered_contact_url = res_or_exc # type: ignore
                    final_records[original_url] = record_main # type: ignore
                    if discovered_contact_url and record_main and not record_main.error: # type: ignore
                        contact_pages_to_fetch.append((original_url, discovered_contact_url))
        
        # --- Phase 2: Contact Page Fetching ---
        if not contact_pages_to_fetch:
            print("\n--- No distinct contact pages discovered or eligible for fetching. ---")
        else:
            print(f"\n--- Round 2: Processing {len(contact_pages_to_fetch)} discovered contact URLs ---")
            round2_tasks = [
                self._fetch_and_extract_contact(orig_url, contact_url, semaphore)
                for orig_url, contact_url in contact_pages_to_fetch
            ]
            if round2_tasks:
                round2_results = await asyncio.gather(*round2_tasks, return_exceptions=True)
                for res_or_exc in round2_results:
                    if isinstance(res_or_exc, Exception):
                        # This error is tricky to map back without more context from the task itself
                        # For now, just log it. Task should ideally return original_url_ref even on error.
                        print(f"CRITICAL Orchestration exception (Round 2 task): {res_or_exc}")
                    else:
                        original_url_ref, actual_contact_url, record_contact_page = res_or_exc # type: ignore
                        
                        if original_url_ref in final_records:
                            main_record_to_update = final_records[original_url_ref]
                            if record_contact_page: # This is the Record object from contact page processing
                                main_record_to_update.merge_with(record_contact_page, actual_contact_url)
                            else: # Should not happen if _fetch_and_extract_contact returns a Record always
                                 main_record_to_update.add_processing_note(f"Contact page {actual_contact_url} processing yielded no data object.")
                        else:
                            print(f"Error: Orphaned contact page result for {actual_contact_url} (original ref {original_url_ref} not found).")
        
        # --- Phase 3: Display and Save Results ---
        print("\n--- Processing Complete. Finalizing Results ---")
        scraped_results_list: List[Record] = []
        # Use initial_urls_from_csv to maintain order and include all attempted URLs
        all_processed_urls_in_order = [url for _, url in initial_urls_from_csv]
        # Add any error URLs from CSV parsing (like empty rows) that weren't in initial_urls_from_csv
        for url_key in final_records:
            if url_key not in all_processed_urls_in_order and url_key.startswith("CSV_Row_"):
                all_processed_urls_in_order.append(url_key)


        for original_url in all_processed_urls_in_order:
            record_instance = final_records.get(original_url)
            if not record_instance:
                # This might happen if an initial URL wasn't processed due to early CSV error, but should be rare
                print(f"Warning: No record found for {original_url} in final results. Creating error placeholder.")
                record_instance = Record.create_error_record(original_url, "Processing record missing")
                final_records[original_url] = record_instance # Add to dict for saving
            
            scraped_results_list.append(record_instance) # For saving

            # Console Output
            data_dict = record_instance.to_dict()
            error_msg = f"Error: {data_dict['error']}" if data_dict['error'] else "OK"
            notes_msg = f"Notes: {'; '.join(data_dict['processing_notes'])}" if data_dict['processing_notes'] else ""
            
            social_summary = []
            for f_name in Record.get_field_names():
                if "_links" in f_name and isinstance(data_dict.get(f_name), list):
                    social_summary.append(f"{f_name.replace('_links','').replace('_',' ').capitalize()}: {len(data_dict.get(f_name,[]))}")
            
            # Find original row number for this URL if possible
            row_num_str = ""
            for rn, u in initial_urls_from_csv:
                if u == original_url:
                    row_num_str = f"(CSV row {rn})"
                    break
            if not row_num_str and original_url.startswith("CSV_Row_"): # For empty URL errors
                 row_num_str = f"({original_url.replace('_Empty','').replace('_',' ')})"


            print(f"Result for {data_dict['url']} {row_num_str}:\n"
                  f"  Phones: {len(data_dict.get('phone_numbers',[]))}, "
                  f"Social: {{ {', '.join(social_summary)} }}, "
                  f"Addresses: {len(data_dict.get('addresses',[]))}, Status: {error_msg}\n"
                  f"  {notes_msg}")
            print("-" * 40)

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

    def _save_results(self, results_to_save: List[Record]): # Takes list of Record objects
        print(f"\nSaving all collected data to {self.output_data_file}...")
        if not results_to_save:
            print("No data (including errors) was collected to save.")
            return

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
            print(f"Error writing to output file '{self.output_data_file}': {e}")
        except Exception as e:
            print(f"An unexpected error occurred during CSV writing: {e}")
            import traceback; traceback.print_exc()


# --- Main Execution ---
if __name__ == "__main__":
    input_file = 'sample-websites.csv' 

    output_file = 'scraped_company_data_two_rounds.csv'
    
    orchestrator = Orchestrator(input_csv_file=input_file, 
                                output_data_file=output_file,
                                concurrent_requests=100) # Concurrency for each round
    orchestrator.run()