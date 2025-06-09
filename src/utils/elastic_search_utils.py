import logging
import re
import time
from typing import List, Optional
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
import phonenumbers

from src.utils import config

log = logging.getLogger(__name__)

# --- Normalization Functions ---

# TODO: Extract normalization patterns to a separate config file or module for easier management.
# TODO: Think what's worth to normalize from html data extractor and also extract methods from there if needed.

SOCIAL_MEDIA_PATTERNS = {
    "facebook": re.compile(r"facebook\.com/([a-zA-Z0-9._-]+/?)(?!.*\b(?:sharer|plugins|events|groups|notes|photo)\b)"),
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
    try:
        # Assuming US as the default region if the number is not international
        parsed_number = phonenumbers.parse(phone_str, "US")
        if phonenumbers.is_valid_number(parsed_number):
            return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.phonenumberutil.NumberParseException:
        log.warning(f"Could not parse phone number: {phone_str}", exc_info=False)
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


# --- Elasticsearch Client and Index Management ---

def get_es_client() -> Elasticsearch:
    """
    Creates and returns a singleton Elasticsearch client, retrying on connection failure.
    """
    for i in range(5):
        try:
            client = Elasticsearch(
                hosts=config.ELASTICSEARCH_HOSTS
            )
            if client.ping():
                log.info("Successfully connected to Elasticsearch.")
                return client
            else:
                raise ConnectionError("Elasticsearch ping failed.")
        except Exception as e:
            log.warning(f"Elasticsearch not available, retrying ({i+1}/5)... Error: {e}")
            time.sleep(5)
    raise ConnectionError("Could not connect to Elasticsearch after multiple retries.")

def create_index_with_mapping():
    """
    Creates the Elasticsearch index with the advanced mapping and custom analyzer.
    This function is idempotent and safe to call on startup.
    """
    client = get_es_client()
    index_name = config.ELASTICSEARCH_INDEX_NAME
    
    if client.indices.exists(index=index_name):
        log.info(f"Index '{index_name}' already exists. No action taken.")
        return

    log.info(f"Index '{index_name}' not found. Creating it now with advanced mapping...")
    
    settings = {
        "analysis": {
            "analyzer": {
                "company_name_analyzer": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "asciifolding"  # Removes accents (e.g., é -> e)
                    ]
                }
            }
        }
    }
    
    mappings = {
        "properties": {
            # Raw fields for display
            "url": {"type": "keyword"},
            "company_name": {"type": "text"},
            "phone_numbers": {"type": "keyword"},
            "social_media_links": {"type": "keyword"},
            "addresses": {"type": "text"},
            
            # Normalized fields for high-quality matching
            "domain": {"type": "keyword"},
            "social_media_profiles": {"type": "keyword"},
            "normalized_phone_numbers": {"type": "keyword"},
            "searchable_name": {
                "type": "text",
                "analyzer": "company_name_analyzer"
            }
        }
    }
    
    try:
        # ignore=400 handles the race condition where another service instance creates the index
        # between our `exists` check and this `create` call.
        client.indices.create(
            index=index_name,
            settings=settings,
            mappings=mappings,
            ignore=400
        )
        log.info(f"Index '{index_name}' created successfully.")
    except Exception as e:
        log.critical(f"Failed to create index '{index_name}'. Error: {e}", exc_info=True)
        raise