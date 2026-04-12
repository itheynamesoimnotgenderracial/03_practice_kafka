#!/bin/sh
# No set -e — nc returns non-zero when Kafka isn't ready yet,
# which would kill the script before the loop completes

echo "⏳ Waiting for Kafka..."

until nc -z kafka1 29092; do
  echo "Kafka not ready yet..."
  sleep 3
done

sleep 5

echo "✅ Kafka is ready. Starting processor..."
exec "$@"
