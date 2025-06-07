import csv
import re
import logging
import time

# Import and call the setup function at the VERY TOP.
from src.utils.logging_setup import setup_logging
setup_logging("URLProducer")

# Now, import the rest of your application modules.
from src.utils import config, kafka_utils

# Use a specific logger for this service's messages.
log = logging.getLogger("URLProducer")

def _prepare_url(url_input: str) -> str:
    """Prepares and normalizes a URL from input."""
    url_input = url_input.strip()
    if not url_input:
        return ""
    if not re.match(r'^[a-zA-Z]+://', url_input):
        return 'http://' + url_input
    return url_input

def main():
    """Main function to produce URLs to Kafka from a CSV."""
    log.info("Starting URL Producer...")
    try:
        producer = kafka_utils.get_kafka_producer()
    except ConnectionError as e:
        log.error(f"Failed to start producer: {e}")
        return

    urls_sent_count = 0
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

                prepared_url = _prepare_url(raw_url)
                message = {"url": prepared_url}
                
                producer.send(config.TOPIC_URLS_TO_FETCH, value=message)
                log.info(f"Sent URL to Kafka: {prepared_url}")
                urls_sent_count += 1

    except FileNotFoundError:
        log.error(f"Input file not found: {config.INPUT_CSV_FILE}")
    except Exception as e:
        log.error(f"An unexpected error occurred", exc_info=True)
    finally:
        if 'producer' in locals() and producer:
            log.info("Flushing remaining messages...")
            producer.flush()
            producer.close()
            log.info("Producer closed.")

    log.info(f"--- URL Producer finished. Sent {urls_sent_count} URLs. ---")

if __name__ == "__main__":
    try:
        main()
    finally:
        log.info("Shutting down logging system...")
        logging.shutdown()