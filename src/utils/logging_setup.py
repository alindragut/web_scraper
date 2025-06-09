import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from src.utils import config

def setup_app_logging(service_name: str, level: int = logging.INFO):
    """
    Sets up the entire logging system for a service.
    
    - Configures a basic console handler.
    - If LOG_TO_KAFKA is 'true', it dynamically imports and adds the KafkaLogHandler.
    - Avoids all circular import issues.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Clear any existing handlers to avoid duplicates
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    if os.environ.get("LOG_TO_KAFKA", "false").lower() == "true":
        try:
            from src.utils.kafka_log_handler import KafkaLogHandler
            
            kafka_handler = KafkaLogHandler(
                service_name=service_name, 
                bootstrap_servers=config.KAFKA_BROKER_URL, 
                topic=config.TOPIC_LOG_EVENTS)
            
            root_logger.addHandler(kafka_handler)
            logging.getLogger(service_name).info("Kafka logging has been ENABLED.")
        except Exception as e:
            logging.getLogger(service_name).critical(
                f"Failed to initialize KafkaLogHandler. Kafka logging is DISABLED. Error: {e}",
                exc_info=True
            )
    else:
        logging.getLogger(service_name).info("Kafka logging is DISABLED.")
        
def setup_log_file_writer() -> logging.Logger:
    """
    Configures and returns a dedicated logger for writing consumed log events to a file.
    
    This is for the Logging Service, which consumes log events. It uses a 
    rotating file handler and a minimal formatter to write the raw JSON log message.
    """
    os.makedirs(config.LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(config.LOG_DIR, config.LOG_FILENAME)

    # Get a dedicated logger. This name will not conflict with any other logger.
    file_writer_log = logging.getLogger("LogEventWriter")
    file_writer_log.setLevel(logging.INFO)
    
    # Use a RotatingFileHandler for robust log management.
    handler = RotatingFileHandler(
        log_file_path, 
        maxBytes=config.LOG_MAX_BYTES
    )
    
    file_formatter = logging.Formatter('%(message)s')
    handler.setFormatter(file_formatter)
    
    # Prevent this logger from sending its messages to the root logger's
    # handlers (console in this case), which would cause duplicate output.
    file_writer_log.propagate = False
    
    file_writer_log.addHandler(handler)
    
    # This is printed to the console for visibility.
    logging.info(f"Log file writer configured. Writing consumed events to: {log_file_path}")
    
    return file_writer_log