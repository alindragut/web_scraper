import logging
import json

from src.utils.logging_setup import setup_app_logging
setup_app_logging("StorageService")

from src.utils import config, kafka_utils, elastic_search_utils as es_utils

log = logging.getLogger("StorageService")

def main():
    """
    Main function for the Storage Service.
    Consumes extracted company data, normalizes it, and stores it in Elasticsearch.
    """
    log.info("Starting Storage Service...")

    try:
        es_client = es_utils.get_es_client()
        es_utils.create_index_with_mapping()
        
        consumer = kafka_utils.get_kafka_consumer(
            topics=[config.TOPIC_EXTRACTED_DATA, config.TOPIC_COMPANY_NAME_DATA],
            group_id=config.STORAGE_GROUP_ID
        )
    except Exception as e:
        log.critical(f"Could not connect to dependencies. Shutting down. Error: {e}")
        return

    log.info(f"Waiting for extracted data on topics: {config.TOPIC_EXTRACTED_DATA}, {config.TOPIC_COMPANY_NAME_DATA}")
    
    try:
        while True:
            msg = consumer.poll(config.KAFKA_CONSUMER_TIMEOUT_SECONDS)
            
            if msg is None:
                continue
            
            if msg.error():
                log.error(f"Error in consumed message: {msg.error()}")
                continue
    
            topic = msg.topic()
            
            try:
                if topic == config.TOPIC_EXTRACTED_DATA:
                    record = json.loads(msg.value().decode('utf-8'))
                    domain = es_utils.get_domain_from_url(record.get("url"))
                    
                    if not domain:
                        log.warning(f"Skipping record with no valid domain: {record.get('url')}")
                        continue
                        
                    doc = {
                        "url": record.get("url"),
                        "phone_numbers": record.get("phone_numbers", []),
                        "social_media_links": record.get("social_media_links", []),
                        "addresses": record.get("addresses", []),
                        "domain": domain,
                        "social_media_profiles": [es_utils.normalize_social_media_profile(link) for link in record.get("social_media_links", []) if link],
                        "normalized_phone_numbers": [es_utils.normalize_phone_number(p) for p in record.get("phone_numbers", []) if p]
                    }
                    
                    es_client.update(
                        index=config.ELASTICSEARCH_INDEX_NAME,
                        id=domain,
                        doc=doc,
                        doc_as_upsert=True
                    )
                    
                    log.info(f"Upserted scraped data for domain: {domain}")
                elif topic == config.TOPIC_COMPANY_NAME_DATA:
                    record = json.loads(msg.value().decode('utf-8'))
                    domain = record.get("domain")
                    company_name = record.get("company_name")

                    if not domain or not company_name:
                        continue
                    
                    doc_to_update = {
                        "company_name": company_name,
                        "searchable_name": es_utils.normalize_company_name(company_name)
                    }
                    
                    es_client.update(
                        index=config.ELASTICSEARCH_INDEX_NAME,
                        id=domain,
                        doc=doc_to_update,
                        doc_as_upsert=True
                    )
                    log.info(f"Upserted company name for domain: {domain}")
                
                consumer.commit(asynchronous=False)
            except Exception as e:
                log.error(f"Failed to process message from topic '{topic}': {e}", exc_info=True)
    except KeyboardInterrupt:
        log.info("Storage Service shutting down...")
    finally:
        consumer.close()
        log.info("Storage Service has been shut down.")

if __name__ == "__main__":
    main()