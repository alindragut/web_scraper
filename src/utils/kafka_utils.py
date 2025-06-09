import logging
from confluent_kafka import Producer, Consumer, KafkaException
import time

from src.utils import config

logger = logging.getLogger(__name__)

RETRIES = 5

def get_kafka_producer() -> Producer:
    """
    Creates and returns a Confluent Kafka Producer.
    """
    conf = {'bootstrap.servers': config.KAFKA_BROKER_URL}
    
    for i in range(RETRIES):
        try:
            producer = Producer(conf)
            # The producer connects lazily, but we can poll for the broker list
            # to check the connection. A timeout of 5 seconds is reasonable.
            producer.list_topics(timeout=5)
            logger.info("Successfully connected to Kafka.")
            return producer
        except KafkaException as e:
            logger.warning(f"Kafka not available, retrying ({i + 1}/{RETRIES}). Error: {e}")
            time.sleep(5)
    raise ConnectionError("Could not connect to Kafka after multiple retries.")


def get_kafka_consumer(topics: list[str], group_id: str) -> Consumer:
    """
    Creates and returns a Confluent Kafka Consumer.
    """
    conf = {
        'bootstrap.servers': config.KAFKA_BROKER_URL,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 300000,
        'max.poll.interval.ms': 360000,
        'partition.assignment.strategy': 'roundrobin'
    }
    
    def on_assign(consumer, partitions):
        member_id = consumer.memberid()
        logger.info(f"SUCCESS: Partitions assigned to consumer {member_id}: {partitions}")

    def on_revoke(consumer, partitions):
        member_id = consumer.memberid()
        logger.warning(f"Partitions revoked from consumer {member_id}: {partitions}")
        
    consumer = Consumer(conf)
    
    consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)
    logger.info(f"KafkaConsumer created and subscribed to topics '{topics}' and group '{group_id}'")
    return consumer