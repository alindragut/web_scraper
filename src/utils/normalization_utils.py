import logging
import re
from typing import Optional
from urllib.parse import urlparse, unquote

import phonenumbers

log = logging.getLogger(__name__)

SOCIAL_MEDIA_PATTERNS = {
    "facebook": re.compile(r"facebook\.com/((?!.*\b(?:sharer|plugins|events|groups|notes|photo)\b)[a-zA-Z0-9._/-]+)"),
    "twitter": re.compile(r"(?:twitter|x)\.com/([a-zA-Z0-9_]{1,15})(?!\b(?:intent|share|search)\b)"),
    "linkedin": re.compile(r"linkedin\.com/(?:company/|in/)([a-zA-Z0-9._-]+/?)"),
    "instagram": re.compile(r"instagram\.com/([a-zA-Z0-9._]+/?)(?!.*\b(?:p/|explore)\b)"),
    "youtube": re.compile(r"youtube\.com/(?:user/|channel/|c/)?([a-zA-Z0-9._-]+/?)")
}

def normalize_social_media_profile(url: str) -> Optional[str]:
    """
    Extracts a normalized profile identifier from a social media URL.
    Returns a string like 'platform:profile_id', e.g., 'facebook:google'.
    Returns None if no specific profile is found.
    """
    if not url:
        return None

    for platform, pattern in SOCIAL_MEDIA_PATTERNS.items():
        match = pattern.search(url)
        if match:
            # The profile identifier is usually the first capturing group
            profile_id = match.group(1).strip('/').lower()
            if profile_id:
                return f"{platform}:{profile_id}"
    return None

def normalize_phone_number(phone_str: str) -> Optional[str]:
    """
    Normalizes a phone number to E.164 format.
    Returns None if the number is invalid.
    """
    if not phone_str:
        return None
    
    try:
        # Decode any URL-encoded characters in the phone string
        decoded_phone_str = unquote(phone_str.strip())
        # Assuming US as the default region if the number is not international
        parsed_number = phonenumbers.parse(decoded_phone_str, "US")
        if phonenumbers.is_valid_number(parsed_number):
            return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.phonenumberutil.NumberParseException:
        log.warning(f"Could not parse phone number: {phone_str}", exc_info=False) # Log original phone string for context
    return None

def get_domain_from_url(url: str) -> Optional[str]:
    """
    Extracts the clean domain (e.g., 'example.com') from a full URL.
    Filters out common, non-informative domains.
    """
    if not url:
        return None
        
    GENERIC_DOMAINS = {'google.com', 'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com'}
    
    try:
        # Prepend http if scheme is missing for urlparse to work correctly
        if '://' not in url:
            url = 'http://' + url
        
        parsed_url = urlparse(url)
        netloc = parsed_url.netloc
        
        # Remove 'www.' prefix if it exists
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        
        if netloc and netloc not in GENERIC_DOMAINS:
            return netloc.lower()
    except Exception:
        log.warning(f"Could not parse domain from URL: {url}", exc_info=False)
    return None

def normalize_company_name(name: str) -> str:
    """
    Cleans a company name for searching by lowercasing, removing symbols,
    and trimming common legal suffixes.
    """
    if not name:
        return ""
    
    # Remove common suffixes using a regex for whole words
    suffixes = ['inc', 'llc', 'ltd', 'p.c', 'pty', 'corporation', 'company']
    name = name.lower()
    # This regex ensures we only match whole words
    for suffix in suffixes:
        name = re.sub(r'\b' + re.escape(suffix) + r'\b\.?', '', name, flags=re.IGNORECASE)

    # Remove all non-alphanumeric characters (except spaces)
    name = re.sub(r'[^\w\s]', '', name)
    # Replace multiple spaces with a single space and strip
    return ' '.join(name.split())

def prepare_url(url_input: str) -> str:
    """Prepares and normalizes a URL from input."""
    url_input = url_input.strip()
    if not url_input:
        return ""
    if not re.match(r'^[a-zA-Z]+://', url_input):
        return 'http://' + url_input
    return url_input