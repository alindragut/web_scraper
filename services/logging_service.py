import logging
# Import and call the setup function at the VERY TOP.
from src.utils.logging_setup import setup_app_logging, setup_log_file_writer

# Logging setup for the Logging Service's own logs.
setup_app_logging("LoggingService")

from src.utils import config, kafka_utils

log = logging.getLogger("LoggingService")

def main():
    """
    Consumes log messages from the 'log_events' topic and prints them.
    This is the final destination for (almost) all logs in the system.
    """
    log.info("Starting Logging Service")
    
    log_file_writer = setup_log_file_writer()
    
    try:
        consumer = kafka_utils.get_kafka_consumer(
            topics=[config.TOPIC_LOG_EVENTS],
            group_id=config.LOGGER_GROUP_ID
        )
    except Exception:
        log.critical("Could not create Kafka consumer. Shutting down.", exc_info=True)
        return

    log.info(f"Listening for messages on topic '{config.TOPIC_LOG_EVENTS}'")

    try:
        while True:
            msg = consumer.poll(config.KAFKA_CONSUMER_TIMEOUT_SECONDS)
            
            if msg is None:
                continue
            
            if msg.error():
                log.error(f"Error in message: {msg.error()}")
                continue
            
            log_message_json = msg.value().decode('utf-8')
            log_file_writer.info(log_message_json)
            
            consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        log.warning("Logging Service shutting down.")
    except Exception:
        log.critical("An error occurred in the Logging Service consumer loop.", exc_info=True)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()