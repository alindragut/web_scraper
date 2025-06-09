import re
import logging
from urllib.parse import urlparse, urljoin, unquote
from bs4 import BeautifulSoup, NavigableString
from typing import Set, Tuple
from src.models.company_record import CompanyRecord

# Get a logger instance for this specific module.
log = logging.getLogger(__name__)

class HtmlDataExtractor:
    PHONE_REGEX = re.compile(r'''
        (\+?\d{1,3}[\s.-]?)? (\(?\d{2,5}\)?[\s.-]?)? (\d{2,5}[\s.-]?){1,2} (\d{3,5}) (?!\d)
    ''', re.VERBOSE)
    SOCIAL_MEDIA_PATTERNS = {
        "facebook": re.compile(r"facebook\.com/((?!.*\b(?:sharer|plugins|events|groups|notes|photo)\b)[a-zA-Z0-9._/-]+)"),
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
    def _normalize_url(url: str, base_url: str) -> str:
        parsed_url = urlparse(urljoin(base_url, url.strip()))
        scheme = parsed_url.scheme if parsed_url.scheme else urlparse(base_url).scheme
        if not scheme or scheme.lower() not in ['http', 'https']: scheme = 'http'
        return f"{scheme}://{parsed_url.netloc}{parsed_url.path}".strip('/')
        
    @staticmethod
    def _clean_phone_number(number_str: str) -> str:
        return re.sub(r'[\s().-]', '', unquote(number_str))

    def _extract_phone_numbers(self, soup: BeautifulSoup, page_text: str) -> Set[str]:
        phones = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('tel:'):
                phones.add(self._clean_phone_number(href.replace('tel:', '').strip()))
        for match_groups in self.PHONE_REGEX.findall(page_text):
            full_match = "".join(filter(None, match_groups))
            if len(re.sub(r'\D', '', full_match)) >= 7:
                phones.add(self._clean_phone_number(full_match))
        return phones

    def _extract_social_media_links(self, soup: BeautifulSoup, page_text, base_url: str) -> Set[str]:
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
        
        for _, pattern in self.SOCIAL_MEDIA_PATTERNS.items():
            # Find all non-overlapping matches in the text
            for match in pattern.finditer(page_text):
                # Re-construct the full URL from the match
                full_url = "https://" + match.group(0)
                extracted_social_links.add(full_url)
        
        return extracted_social_links

    def _extract_addresses(self, soup: BeautifulSoup, page_text: str) -> Set[str]:
        addresses = set()
        for address_tag in soup.find_all(['address', lambda tag: tag.has_attr('itemprop') and tag['itemprop'] == 'address']):
            address_text = ' '.join(address_tag.get_text(separator=' ', strip=True).split())
            if self.ADDRESS_MIN_LENGTH < len(address_text) < self.ADDRESS_MAX_LENGTH:
                addresses.add(address_text)
                
        for line in page_text.splitlines():
            # A potential address must contain at least one digit.
            # This filters a lot of junk like "Contact us" or "Email address".
            if any(char.isdigit() for char in line):
                line = line.strip()
                if self.ADDRESS_MIN_LENGTH < len(line) < self.ADDRESS_MAX_LENGTH:
                    if any(keyword in line.lower() for keyword in self.ADDRESS_KEYWORDS) or self.BASIC_ADDRESS_PART_REGEX.search(line):
                        addresses.add(line)
                
        return addresses

    def _find_contact_page_urls(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
        """Finds and normalizes potential contact page URLs within a page."""
        contact_urls = set()
        base_domain = urlparse(base_url).netloc

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if not href or href.startswith(('#', 'mailto:', 'javascript:')):
                continue

            # Check for keywords in the href itself or in the link's text
            is_potential = any(keyword in href.lower() for keyword in self.CONTACT_PAGE_KEYWORDS)
            if not is_potential:
                link_text = a_tag.get_text(strip=True)
                if any(pattern.search(link_text) for pattern in self.CONTACT_LINK_TEXT_PATTERNS):
                    is_potential = True

            if is_potential:
                absolute_url = self._normalize_url(href, base_url)
               # Only add internal links that are not the page we are currently on
                if urlparse(absolute_url).netloc == base_domain and absolute_url != base_url:
                    contact_urls.add(absolute_url)
                    
        return contact_urls

    def extract_all_data(self, base_url: str, html_content: str, contact_url: str) -> Tuple[CompanyRecord, Set[str]]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            page_text = ' '.join(soup.stripped_strings)
            
            phone_numbers = self._extract_phone_numbers(soup, page_text)
            social_media_links = self._extract_social_media_links(soup, page_text, base_url)
            text_with_newlines = soup.get_text(separator='\n', strip=True) # Better for address parsing
            addresses = self._extract_addresses(soup, text_with_newlines)
            
            contact_pages = set()
            if not contact_url:
                contact_pages = self._find_contact_page_urls(soup, base_url)
            
            record = CompanyRecord( 
                url=base_url,
                phone_numbers=phone_numbers,
                addresses=addresses,
                social_media_links=social_media_links
            )
            
            return record, contact_pages
        except Exception as e:
            import traceback; traceback.print_exc()
            log.error(f"Unexpected fetch error for {base_url}", exc_info=True)
            return CompanyRecord(url=base_url), set()