import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time

from src.utils import config

# Get a logger instance for this specific module.
logger = logging.getLogger(__name__)

RETRIES = 5  # Number of retries for Kafka connection

def get_kafka_producer() -> KafkaProducer:
    """
    Creates and returns a KafkaProducer. Retries connection on failure.
    """
    for i in range(RETRIES):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[config.KAFKA_BROKER_URL],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Successfully connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not available, retrying ({i + 1}/{RETRIES})")
            time.sleep(5)
    raise ConnectionError("Could not connect to Kafka after multiple retries.")


def get_kafka_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """
    Creates and returns a KafkaConsumer for a given topic and group_id.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[config.KAFKA_BROKER_URL],
        auto_offset_reset='earliest',
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    logger.info(f"KafkaConsumer created for topic '{topic}' and group '{group_id}'")
    return consumer