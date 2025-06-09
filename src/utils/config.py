import os

# Kafka general configuration
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_CONSUMER_TIMEOUT_SECONDS = float(os.environ.get("KAFKA_CONSUMER_TIMEOUT", "1.0"))
KAFKA_CONSUMER_BATCH_SIZE = int(os.environ.get("KAFKA_CONSUMER_BATCH_SIZE", "1024"))

# Elasticsearch general configuration
ELASTICSEARCH_HOSTS = os.environ.get("ELASTICSEARCH_HOSTS", "http://localhost:9200")
ELASTICSEARCH_INDEX_NAME = os.environ.get("ELASTICSEARCH_INDEX_NAME", "company_records")

# Kafka Topics
TOPIC_URLS_TO_FETCH = os.environ.get("KAFKA_URLS_TOPIC", "urls_to_fetch")
TOPIC_HTMLS_TO_PROCESS = os.environ.get("KAFKA_HTMLS_TOPIC", "htmls_to_process")
TOPIC_EXTRACTED_DATA = os.environ.get("KAFKA_EXTRACTED_DATA_TOPIC", "extracted_data")
TOPIC_LOG_EVENTS = os.environ.get("KAFKA_LOGS_TOPIC", "log_events")
TOPIC_COMPANY_NAME_DATA = os.environ.get("KAFKA_COMPANY_NAME_DATA_TOPIC", "company_name_data")

# Kafka Group IDs
FETCHER_GROUP_ID = os.environ.get("KAFKA_FETCHER_GROUP_ID", "fetcher_service_group")
EXTRACTOR_GROUP_ID = os.environ.get("KAFKA_EXTRACTOR_GROUP_ID", "extractor_service_group")
ANALYTICS_GROUP_ID = os.environ.get("KAFKA_ANALYTICS_GROUP_ID", "analytics_service_group")
LOGGER_GROUP_ID = os.environ.get("KAFKA_LOGGER_GROUP_ID", "logging_service_group")
STORAGE_GROUP_ID = os.environ.get("KAFKA_STORAGE_GROUP_ID", "storage_service_group")

# URL Producer
INPUT_CSV_FILE = os.environ.get("INPUT_CSV_FILE", "data/sample-websites.csv")

# Company Name Data Producer
COMPANY_NAME_CSV_FILE = os.environ.get("COMPANY_NAME_CSV_FILE", "data/sample-websites-company-names.csv")

# Fetcher Service
MAX_CONCURRENT_FETCHES = int(os.environ.get("MAX_CONCURRENT_FETCHES", "256"))
URL_FETCH_TIMEOUT_SECONDS = int(os.environ.get("URL_FETCH_TIMEOUT_SECONDS", "15"))

# Analytics Service
ANALYTICS_REPORT_INTERVAL_SECONDS = int(os.environ.get("ANALYTICS_REPORT_INTERVAL_SECONDS", "30"))

# Logging
LOG_DIR = os.environ.get("LOG_DIR", "/var/log/app")
LOG_FILENAME = os.environ.get("LOG_FILENAME", "services.log")
LOG_MAX_BYTES = int(os.environ.get("LOG_MAX_BYTES", 10 * 1024 * 1024))