import csv
import json
import logging

from src.utils.logging_setup import setup_app_logging
setup_app_logging("CompanyNameDataProducer")

from src.utils import config
from src.utils import kafka_utils
from src.utils import normalization_utils

log = logging.getLogger("CompanyNameDataProducer")

def get_best_company_name(row: dict) -> str:
    """
    Selects the best available company name from the CSV row, in order of preference.
    """
    legal = row.get("company_legal_name", "").strip()
    commercial = row.get("company_commercial_name", "").strip()
    all_names_str = row.get("company_all_available_names", "").strip()
    
    if legal: return legal
    if commercial: return commercial
    if all_names_str:
        # Take the first available name from the pipe-separated list
        return all_names_str.split('|')[0].strip()
    return ""

def main():
    """Main function to send company names from a CSV to the Storage Service."""
    log.info("Starting Company Name Data Producer.")
    try:
        producer = kafka_utils.get_kafka_producer()
    except ConnectionError as e:
        log.error(f"Failed to start producer: {e}")
        return

    messages_sent_count = 0
    try:
        with open(config.COMPANY_NAME_CSV_FILE, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                raw_url = row.get("domain", "").strip()
                domain = normalization_utils.get_domain_from_url(raw_url)
                
                if not domain:
                    log.warning(f"Skipping row with invalid or missing domain: {row}")
                    continue

                company_name = get_best_company_name(row)
                if not company_name:
                    log.warning(f"Skipping row with no company name for domain: {domain}")
                    continue
                
                prepared_url = normalization_utils.prepare_url(raw_url)

                message = {
                    "domain": domain,
                    "url": prepared_url,
                    "company_name": company_name
                }
                
                producer.produce(
                    topic=config.TOPIC_COMPANY_NAME_DATA,
                    key=domain.encode('utf-8'), # Use domain as key for partitioning
                    value=json.dumps(message).encode('utf-8')
                )
                
                log.info(f"Sent company name for domain '{domain}': {company_name}")
                messages_sent_count += 1

    except FileNotFoundError:
        log.error(f"Input file not found: {config.COMPANY_NAME_CSV_FILE}")
    except Exception:
        log.error("An unexpected error occurred.", exc_info=True)
    finally:
       if producer is not None:
            log.info("Flushing remaining messages.")
            producer.flush()

    log.info(f"Company Name Data Producer finished. Sent {messages_sent_count} messages.")

if __name__ == "__main__":
    main()