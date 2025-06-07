import logging
import sys
import os
from src.utils.config import KAFKA_BROKER_URL, TOPIC_LOG_EVENTS

def setup_logging(service_name: str, level: int = logging.INFO):
    """
    Sets up the entire logging system for a service.
    
    - Configures a basic console handler.
    - If LOG_TO_KAFKA is 'true', it dynamically imports and adds the KafkaLogHandler.
    - This centralized approach avoids all circular import issues.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Clear any existing handlers to avoid duplicates
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # --- 1. Console Handler (always on) ---
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # --- 2. Kafka Handler (conditional) ---
    # The import is INSIDE the function, which is key to avoiding module-level circular dependencies.
    if os.environ.get("LOG_TO_KAFKA", "true").lower() == "true":
        try:
            # Dynamically import here
            from src.utils.kafka_log_handler import KafkaLogHandler
            
            kafka_handler = KafkaLogHandler(service_name=service_name, bootstrap_servers=KAFKA_BROKER_URL, topic=TOPIC_LOG_EVENTS)
            root_logger.addHandler(kafka_handler)
            logging.getLogger(service_name).info("Kafka logging has been ENABLED.")
        except Exception as e:
            logging.getLogger(service_name).critical(
                f"Failed to initialize KafkaLogHandler. Kafka logging is DISABLED. Error: {e}",
                exc_info=True
            )
    else:
        logging.getLogger(service_name).info("Kafka logging is DISABLED.")