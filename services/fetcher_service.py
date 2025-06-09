import asyncio
import logging
import json
from src.utils.logging_setup import setup_app_logging
from confluent_kafka import Producer

setup_app_logging("FetcherService")

from src.utils import config, kafka_utils
from src.components.web_fetcher import WebFetcher

log = logging.getLogger("FetcherService")


async def fetch_and_process(url: str, web_fetcher: WebFetcher, producer: Producer, semaphore: asyncio.Semaphore):
    """
    A single, self-contained task to fetch one URL and produce the result.
    This function is designed to be run concurrently for many URLs.
    """
    async with semaphore:
    # Use a semaphore to limit the number of concurrent fetches
        log.info(f"Fetching: {url}")
        try:
            html_content_bytes = await web_fetcher.fetch_single_page_async(url)

            if html_content_bytes:
                html_string = html_content_bytes.decode('utf-8', errors='ignore')
                result_payload = {"url": url, "html_content": html_string}
                
                producer.produce(
                    topic=config.TOPIC_HTMLS_TO_PROCESS,
                    value=json.dumps(result_payload).encode('utf-8')
                )
                log.info(f"Successfully fetched and produced: {url}")
            else:
                log.warning(f"Fetch failed for: {url}")
                
        except Exception as e:
            log.error(f"An unexpected error occurred while processing {url}", exc_info=True)
        finally:
            # We poll here to allow the producer to send messages in the background
            # and process delivery callbacks.
            producer.poll(0)


async def main():
    """
    Main async function for the Fetcher Service.
    Consumes URLs in batches and processes them concurrently.
    """
    log.info("Starting High-Performance Fetcher Service...")
    web_fetcher = WebFetcher()
    
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_FETCHES)
    
    try:
        consumer = kafka_utils.get_kafka_consumer(
            topics=[config.TOPIC_URLS_TO_FETCH], 
            group_id=config.FETCHER_GROUP_ID
        )
        producer = kafka_utils.get_kafka_producer()
    except ConnectionError as e:
        log.critical(f"Could not connect to Kafka. Shutting down. Error: {e}")
        return

    log.info(f"Waiting for URLs on topic '{config.TOPIC_URLS_TO_FETCH}'...")
    try:
        while True:
            messages = consumer.consume(num_messages=config.KAFKA_CONSUMER_BATCH_SIZE, timeout=config.KAFKA_CONSUMER_TIMEOUT_SECONDS)
            
            if not messages:
                continue

            log.info(f"Consumed a batch of {len(messages)} messages.")
            
            tasks = []
            for msg in messages:
                if msg.error():
                    log.error(f"Error in message: {msg.error()}")
                    continue
                
                try:
                    url_data = json.loads(msg.value().decode('utf-8'))
                    url = url_data.get("url")
                    if url:
                        # Create an asyncio task for each valid URL
                        task = asyncio.create_task(
                            fetch_and_process(url, web_fetcher, producer, semaphore)
                        )
                        tasks.append(task)
                    else:
                        log.warning(f"Received message without a URL: {url_data}")
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.error(f"Could not decode message: {e}", exc_info=False)

            if tasks:
                log.info(f"Processing batch of {len(tasks)} URLs concurrently...")
                await asyncio.gather(*tasks)
                consumer.commit(asynchronous=False)
                log.info("Finished processing batch.")
    except KeyboardInterrupt:
        log.info("Fetcher Service shutting down due to user interrupt...")
    except Exception as e:
        log.critical(f"A critical error occurred in the Fetcher Service main loop", exc_info=True)
    finally:
        log.info("Closing resources...")
        await web_fetcher.close_session()
        if producer is not None:
            log.info("Flushing remaining messages.")
            producer.flush(10)
        consumer.close()
        log.info("Fetcher Service has been shut down.")

if __name__ == "__main__":
    asyncio.run(main())