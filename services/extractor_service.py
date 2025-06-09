import logging
import json
from src.utils.logging_setup import setup_app_logging

setup_app_logging("ExtractorService")

from src.utils import config, kafka_utils
from src.components.html_data_extractor import HtmlDataExtractor

log = logging.getLogger("ExtractorService")

def main():
    """
    Main function for the Extractor Service.
    Consumes HTML content, extracts structured data, and produces the result.
    """
    log.info("Starting Extractor Service...")
    html_extractor = HtmlDataExtractor()

    try:
        consumer = kafka_utils.get_kafka_consumer(
            topics=[config.TOPIC_HTMLS_TO_PROCESS],
            group_id=config.EXTRACTOR_GROUP_ID
        )
        producer = kafka_utils.get_kafka_producer()
    except ConnectionError as e:
        log.critical(f"Could not connect to Kafka. Shutting down. Error: {e}")
        return

    log.info(f"Waiting for HTML messages on topic '{config.TOPIC_HTMLS_TO_PROCESS}'...")

    try:
        while True:
            messages = consumer.consume(num_messages=config.KAFKA_CONSUMER_BATCH_SIZE, timeout=config.KAFKA_CONSUMER_TIMEOUT_SECONDS)
            
            if not messages:
                continue
            
            processed_messages_count = 0

            log.info(f"Consumed a batch of {len(messages)} messages.")
            
            for msg in messages:
                if msg.error():
                    log.error(f"Error in message: {msg.error()}")
                    continue

                try:
                    html_data = json.loads(msg.value().decode('utf-8'))
                    url = html_data.get("url")
                    html_content = html_data.get("html_content")

                    if not url or html_content is None:
                        log.warning(f"Received malformed message: {html_data}")
                        continue

                    log.info(f"Extracting data from: {url}")
                    
                    record = html_extractor.extract_all_data(url, html_content)

                    producer.produce(
                        topic=config.TOPIC_EXTRACTED_DATA,
                        value=json.dumps(record.to_dict()).encode('utf-8')
                    )
                    
                    processed_messages_count += 1
                except Exception as e:
                    log.error(f"Failed to process message. Error: {e}", exc_info=True)
            
            if processed_messages_count > 0:
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        log.info("Extractor Service shutting down...")
    finally:
        log.info("Closing Kafka resources...")
        producer.flush(10)
        consumer.close()
        log.info("Extractor Service has been shut down.")

if __name__ == "__main__":
    main()