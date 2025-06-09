import logging
import json
import traceback
import sys
import os
from confluent_kafka import Producer

class KafkaLogHandler(logging.Handler):
    """
    A custom logging handler that sends log records to a Kafka topic
    using the robust confluent-kafka-python library.
    """
    def __init__(self, service_name: str, bootstrap_servers: str, topic: str):
        super().__init__()
        self.service_name = service_name
        self.formatter = logging.Formatter('%(asctime)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.topic = os.environ.get("KAFKA_LOG_TOPIC", "log_events")
        
        conf = {
            'bootstrap.servers': bootstrap_servers,
        }
        try:
            self.producer = Producer(conf)
        except Exception as e:
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
            log_entry['exception'] = self.formatter.formatException(record.exc_info)

        try:
            # The produce() method is non-blocking.
            self.producer.produce(
                self.topic,
                value=json.dumps(log_entry).encode('utf-8')
            )
            # We poll to allow delivery reports to be served.
            self.producer.poll(0)
        except Exception as e:
            sys.stderr.write(f"KAFKA LOGGING FAILED (produce call): {e}\n")
            traceback.print_exc(file=sys.stderr)

    def close(self):
        if self.producer:
            # flush() blocks until all outstanding messages are delivered.
            self.producer.flush(10)  # 10 seconds timeout
        super().close()