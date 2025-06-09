import csv
import logging
import json

from src.utils.logging_setup import setup_app_logging
setup_app_logging("URLProducer")

from src.utils import config
from src.utils import kafka_utils
from src.utils import normalization_utils

log = logging.getLogger("URLProducer")

def main():
    """Main function to produce URLs to Kafka from a CSV."""
    log.info("Starting URL Producer")
    try:
        producer = kafka_utils.get_kafka_producer()
    except ConnectionError as e:
        log.error(f"Failed to start producer: {e}")
        return

    urls_sent_count = 0
    unique_urls = set() # Deduplicate URLs
    try:
        with open(config.INPUT_CSV_FILE, 'r', newline='', encoding='utf-8') as csvfile:
            print("Reading CSV file:", config.INPUT_CSV_FILE)
            reader = csv.DictReader(csvfile)
            if "domain" not in (reader.fieldnames or []):
                log.error(f"CSV file '{config.INPUT_CSV_FILE}' must have a 'domain' header.")
                return

            for row in reader:
                raw_url = row.get("domain", "").strip()
                if not raw_url:
                    log.warning("Found empty row in CSV, skipping.")
                    continue

                prepared_url = normalization_utils.prepare_url(raw_url)
                
                if prepared_url in unique_urls:
                    log.warning(f"Duplicate URL found: {prepared_url}, skipping.")
                    continue
                
                message = {"url": prepared_url, "contact_url": ""}
                
                producer.produce(
                    topic=config.TOPIC_URLS_TO_FETCH,
                    value=json.dumps(message).encode('utf-8')
                )
                
                log.info(f"Produced message for URL: {prepared_url}")
                unique_urls.add(prepared_url)
                urls_sent_count += 1

    except FileNotFoundError:
        log.error(f"Input file not found: {config.INPUT_CSV_FILE}")
    except Exception as e:
        log.error(f"An unexpected error occurred.", exc_info=True)
    finally:       
        if producer is not None:
            log.info("Flushing remaining messages.")
            producer.flush()

    log.info(f"Producer finished. Sent {urls_sent_count} URLs.")

if __name__ == "__main__":
    try:
        main()
    finally:
        log.info("Shutting down logging system.")
        logging.shutdown()