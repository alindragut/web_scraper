import logging
import json
import traceback
import sys
from kafka import KafkaProducer

class KafkaLogHandler(logging.Handler):
    """
    A custom logging handler that sends log records to a Kafka topic.
    This handler is self-contained and does not depend on other project utils
    to avoid circular import issues with the logging system.
    """
    def __init__(self, service_name: str, bootstrap_servers: str, topic: str):
        super().__init__()
        self.service_name = service_name
        self.topic = topic
        self.formatter = logging.Formatter(
            '%(asctime)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1,
                retries=0
            )
        except Exception:
            self.producer = None
            traceback.print_exc(file=sys.stderr)

    def emit(self, record: logging.LogRecord):
        if not self.producer:
            return

        log_entry = {
            "timestamp": self.formatter.formatTime(record, self.formatter.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "service": self.service_name,
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        try:
            # Use the topic defined during initialization
            self.producer.send(self.topic, value=log_entry)
        except Exception:
            sys.stderr.write("--- KAFKA LOGGING FAILED ---\n")
            traceback.print_exc(file=sys.stderr)
            sys.stderr.write(json.dumps(log_entry) + "\n")
            sys.stderr.write("---------------------------\n")

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
        super().close()