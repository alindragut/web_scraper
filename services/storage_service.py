import logging
import json

from src.utils.logging_setup import setup_app_logging
setup_app_logging("StorageService")

from src.utils import config, kafka_utils, normalization_utils, elastic_search_utils as es_utils

log = logging.getLogger("StorageService")

def main():
    """
    Main function for the Storage Service.
    Consumes extracted company data, normalizes it, and stores it in Elasticsearch.
    """
    log.info("Starting Storage Service")

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
            
            try:
                topic = msg.topic()
                record = json.loads(msg.value().decode('utf-8'))
                url = record.get("url")
                domain = normalization_utils.get_domain_from_url(url or record.get("domain"))

                if not domain:
                    log.warning(f"Skipping record with no valid domain: {record}")
                    continue

                # This script merges data. It adds new items to lists
                # (avoiding duplicates) and overwrites non-list fields like company_name.
                script_source = """
                    for (key in params.data.keySet()) {
                        if (params.data[key] instanceof List) {
                            if (ctx._source[key] == null) {
                                ctx._source[key] = [];
                            }
                            for (item in params.data[key]) {
                                if (item != null && !ctx._source[key].contains(item)) {
                                    ctx._source[key].add(item);
                                }
                            }
                        } else if (params.data[key] != null) {
                            ctx._source[key] = params.data[key];
                        }
                    }
                """
                
                # This is the document that gets created if it doesn't exist.
                # It's a complete skeleton to ensure consistent structure.
                upsert_doc = {
                    "url": url, 
                    "company_name": None, 
                    "searchable_name": None,
                    "phone_numbers": [], 
                    "social_media_links": [], 
                    "addresses": [],
                    "domain": domain, 
                    "social_media_profiles": [], 
                    "normalized_phone_numbers": [],
                }
                
                if topic == config.TOPIC_EXTRACTED_DATA:
                    data_to_merge = {
                        "url": record.get("url"),
                        "phone_numbers": record.get("phone_numbers", []), 
                        "social_media_links": record.get("social_media_links", []),
                        "addresses": record.get("addresses", []), 
                        "social_media_profiles": [p for p in [normalization_utils.normalize_social_media_profile(link) for link in record.get("social_media_links", [])] if p],
                        "normalized_phone_numbers": [p for p in [normalization_utils.normalize_phone_number(p) for p in record.get("phone_numbers", [])] if p]
                    }
                elif topic == config.TOPIC_COMPANY_NAME_DATA:
                    data_to_merge = {
                        "company_name": record.get("company_name"), 
                        "url": record.get("url"),
                        "searchable_name": normalization_utils.normalize_company_name(record.get("company_name"))
                    }
                
                upsert_doc.update(data_to_merge)
                
                es_client.update(
                    index=config.ELASTICSEARCH_INDEX_NAME,
                    id=domain,
                    body={
                        "scripted_upsert": True,
                        "script": { "source": script_source, "lang": "painless", "params": {"data": data_to_merge} },
                        "upsert": upsert_doc
                    }
                )
                log.info(f"Merged data for domain '{domain}' from topic '{topic}'")
                
                consumer.commit(asynchronous=False)
            except Exception as e:
                log.error(f"Failed to process message from topic '{topic}': {e}", exc_info=True)
    except KeyboardInterrupt:
        log.info("Storage Service shutting down.")
    finally:
        consumer.close()
        log.info("Storage Service has been shut down.")

if __name__ == "__main__":
    main()