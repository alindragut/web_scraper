import os

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")

TOPIC_URLS_TO_FETCH = os.environ.get("KAFKA_URLS_TOPIC", "urls_to_fetch")
TOPIC_HTMLS_TO_PROCESS = os.environ.get("KAFKA_HTMLS_TOPIC", "htmls_to_process")
TOPIC_EXTRACTED_DATA = os.environ.get("KAFKA_EXTRACTED_DATA_TOPIC", "extracted_data")
TOPIC_LOG_EVENTS = os.environ.get("KAFKA_LOGS_TOPIC", "log_events")

FETCHER_GROUP_ID = "fetcher_service_group"

INPUT_CSV_FILE = "sample-websites.csv"