#!/bin/sh
set -e

KAFKA_BROKER_URL="${KAFKA_BROKER_URL:-kafka:29092}"
REQUIRED_TOPIC="${KAFKA_LOGS_TOPIC:-log_events}"

echo "Entrypoint: Waiting for Kafka topic '$REQUIRED_TOPIC' to be available..."

until kafkacat -b "$KAFKA_BROKER_URL" -L -t "$REQUIRED_TOPIC" | grep "1 partitions"; do
  >&2 echo "Entrypoint: Kafka topic '$REQUIRED_TOPIC' is not available yet - sleeping"
  sleep 2
done

>&2 echo "Entrypoint: Kafka topic '$REQUIRED_TOPIC' is ready. Starting application."

exec "$@"