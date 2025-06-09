import re
import logging
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from typing import List, Set
from src.models.company_record import CompanyRecord

# Get a logger instance for this specific module.
log = logging.getLogger(__name__)

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

    def _extract_social_media_links(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
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
                        profile_part = match.group(1)
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


    def extract_all_data(self, current_page_url: str, html_content: str) -> CompanyRecord:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()
            page_text = ' '.join(soup.stripped_strings)
            
            phone_numbers = self._extract_phone_numbers(soup, page_text)
            social_media_links = self._extract_social_media_links(soup, current_page_url)
            addresses = self._extract_addresses(soup, page_text)
            
            # This record is temporary - its URL will be overwritten or used for merging.
            return CompanyRecord( 
                url=current_page_url, # Temporary, will be used for matching or logging in Orchestrator
                phone_numbers=phone_numbers,
                addresses=addresses,
                social_media_links=social_media_links
            )
        except Exception as e:
            import traceback; traceback.print_exc()
            log.error(f"Unexpected fetch error for {current_page_url}", exc_info=True)
            return CompanyRecord(url=current_page_url)