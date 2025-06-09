import logging
import time

from elasticsearch import Elasticsearch

from src.utils import config

log = logging.getLogger(__name__)

_es_client = None

def get_es_client() -> Elasticsearch:
    """
    Creates and returns a singleton Elasticsearch client, retrying on connection failure.
    """
    global _es_client
    
    if _es_client:
        return _es_client
    
    for i in range(5):
        try:
            client = Elasticsearch(
                hosts=config.ELASTICSEARCH_HOSTS
            )
            if client.ping():
                log.info("Successfully connected to Elasticsearch.")
                _es_client = client
                return client
            else:
                raise ConnectionError("Elasticsearch ping failed.")
        except Exception as e:
            log.warning(f"Elasticsearch not available, retrying ({i+1}/5). Error: {e}")
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

    log.info(f"Index '{index_name}' not found. Creating it now with advanced mapping.")
    
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