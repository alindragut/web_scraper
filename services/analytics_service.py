import logging
import json
import time

from src.utils.logging_setup import setup_app_logging
setup_app_logging("AnalyticsService")

from src.utils import config, kafka_utils
from src.components.pipeline_metrics_tracker import PipelineMetricsTracker

log = logging.getLogger("AnalyticsService")


def main():
    """
    Main function for the Analytics Service.
    This service continuously monitors the pipeline's performance by consuming
    messages from the log events and extracted data topics, and generates metrics
    reports at regular intervals.
    """
    log.info("Starting Analytics Service for continuous monitoring")
    
    tracker = PipelineMetricsTracker()
    
    try:
        # Create a single consumer for both topics. Ensure auto-commit is enabled
        # to allow for real-time processing without manual commits.
        consumer = kafka_utils.get_kafka_consumer(
            topics=[config.TOPIC_LOG_EVENTS, config.TOPIC_EXTRACTED_DATA],
            group_id=config.ANALYTICS_GROUP_ID
        )
    except Exception as e:
        log.critical("Could not create Kafka consumer. Shutting down.", exc_info=True)
        return

    last_report_time = time.time()
    
    log.info(f"Consuming from topics: {config.TOPIC_LOG_EVENTS}, {config.TOPIC_EXTRACTED_DATA}")
    try:
        while True:
            msg = consumer.poll(config.KAFKA_CONSUMER_TIMEOUT_SECONDS)
            if msg is None:
                # Check if it's time to generate a report
                if time.time() - last_report_time > config.ANALYTICS_REPORT_INTERVAL_SECONDS:
                    report = tracker.generate_report()
                    log.info(json.dumps(report))
                    last_report_time = time.time()
                continue
            
            if msg.error():
                log.error(f"Error in message: {msg.error()}")
                continue
            
            topic = msg.topic()
            try:
                if topic == config.TOPIC_LOG_EVENTS:
                    log_entry = json.loads(msg.value().decode('utf-8'))
                    tracker.process_log_event(log_entry)
                elif topic == config.TOPIC_EXTRACTED_DATA:
                    record = json.loads(msg.value().decode('utf-8'))
                    tracker.process_extracted_data(record)
            except (json.JSONDecodeError, AttributeError):
                log.warning("Could not parse a message. Skipping.")
            finally:
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        log.warning("Analytics Service shutting down.")
    finally:
        final_report = tracker.generate_report()
        log.info("Final metrics report:")
        log.info(json.dumps(final_report))
        consumer.close()
        log.info("Analytics Service has been shut down.")

if __name__ == "__main__":
    main()