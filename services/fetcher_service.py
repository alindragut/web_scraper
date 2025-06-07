import asyncio
import logging
import os
from src.utils.logging_setup import configure_basic_logging

# Configure logging like this to ensure it is set up before any other imports that might log messages.
configure_basic_logging()

# Check if we should log to Kafka
if os.environ.get("LOG_TO_KAFKA", "false").lower() == "true":
    try:
        from src.utils.kafka_log_handler import KafkaLogHandler
        
        kafka_handler = KafkaLogHandler(service_name="FetcherService")
        
        # Add the handler to the root logger. All loggers will inherit it.
        logging.getLogger().addHandler(kafka_handler)
        logging.info("Kafka logging has been ENABLED and added to the root logger.")
    except Exception as e:
        logging.critical(f"Failed to initialize KafkaLogHandler. Kafka logging is DISABLED. Error: {e}", exc_info=True)

from src.utils import config, kafka_utils
from src.components.web_fetcher import WebFetcher

log = logging.getLogger("FetcherService")

async def main():
    """
    Main async function for the Fetcher Service.
    Consumes URLs, fetches content, and produces results to the next topic.
    """
    log.info("Starting Fetcher Service...")
    web_fetcher = WebFetcher()
    
    consumer = kafka_utils.get_kafka_consumer(
        config.TOPIC_URLS_TO_FETCH, config.FETCHER_GROUP_ID
    )
    # Get a producer and use a helper to log its connection status
    try:
        producer = kafka_utils.get_kafka_producer()
    except ConnectionError as e:
        log.critical(f"Could not connect to Kafka. Shutting down. Error: {e}")
        return

    log.info(f"Waiting for URLs on topic '{config.TOPIC_URLS_TO_FETCH}'...")
    try:
        for message in consumer:
            url_data = message.value
            url = url_data.get("url")

            if not url:
                log.warning(f"Received message without a URL: {url_data}")
                continue

            log.info(f"Processing URL: {url}")
            html_content_bytes = await web_fetcher.fetch_single_page_async(url)

            if html_content_bytes:
                try:
                    html_string = html_content_bytes.decode('utf-8', errors='ignore')
                    result_payload = {"url": url, "html_content": html_string}
                    producer.send(config.TOPIC_HTML_TO_PROCESS, value=result_payload)
                    log.info(f"Successfully fetched and produced HTML for {url}")
                except Exception as e:
                    log.error(f"Error producing successful fetch for {url}", exc_info=True)
            else:
                log.warning(f"Fetch failed for {url}.")
            
            producer.flush()

    except KeyboardInterrupt:
        log.info("Fetcher Service shutting down due to user interrupt...")
    except Exception as e:
        log.critical(f"A critical error occurred in the Fetcher Service main loop", exc_info=True)
    finally:
        await web_fetcher.close_session()
        consumer.close()
        producer.close()
        log.info("Fetcher Service has been shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This will catch a Ctrl+C if it happens during initial setup
        log.info("Process interrupted by user.")