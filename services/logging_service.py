import json
import logging
# Import and call the setup function at the VERY TOP.
from src.utils.logging_setup import setup_logging
setup_logging("LoggingService") # <<< This one call replaces all the old setup logic.

from src.utils import config, kafka_utils

log = logging.getLogger("LoggingService")

def main():
    """
    Consumes log messages from the 'log_events' topic and prints them.
    This is the final destination for (almost) all logs in the system.
    """
    log.info("Starting Logging Service...")
    
    try:
        consumer = kafka_utils.get_kafka_consumer(
            topic=config.TOPIC_LOG_EVENTS,
            group_id="logging_service_group" # Unique group ID
        )
    except Exception:
        log.critical("Could not create Kafka consumer. Shutting down.", exc_info=True)
        return

    log.info(f"Listening for messages on topic '{config.TOPIC_LOG_EVENTS}'...")

    try:
        for message in consumer:
            log_data = message.value
            # Pretty-print the received JSON log
            print(json.dumps(log_data, indent=2))
    except KeyboardInterrupt:
        log.warning("Logging Service shutting down.")
    except Exception:
        log.critical("An error occurred in the Logging Service consumer loop.", exc_info=True)
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    main()