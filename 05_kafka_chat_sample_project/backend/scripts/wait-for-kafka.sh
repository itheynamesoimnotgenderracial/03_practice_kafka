#!/bin/sh
set -e

echo "⏳ Waiting for Kafka..."

until nc -z kafka1 29092; do
  echo "Kafka not ready yet..."
  sleep 3
done

sleep 5

echo "✅ Kafka is ready. Starting processor..."
exec "$@"
