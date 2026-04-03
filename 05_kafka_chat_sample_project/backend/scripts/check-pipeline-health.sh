#!/bin/bash

BROKER="kafka1:29092"

echo "════════════════════════════════════════"
echo "  Kafka Pipeline Health Check"
echo "════════════════════════════════════════"

echo ""
echo "📊 Consumer Lag (all groups)"
echo "────────────────────────────"
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server $BROKER \
  --all-groups \
  --describe 2>/dev/null | grep -v "^$" | grep -v "^GROUP"

echo ""
echo "🔄 Consumer Group States"
echo "────────────────────────"
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server $BROKER \
  --all-groups \
  --describe \
  --state 2>/dev/null | grep -v "^$"

echo ""
echo "📬 Topic Latest Offsets"
echo "────────────────────────"
for topic in chat.raw chat.validated chat.timeline chat.raw.dlt chat.raw.retry; do
  printf "%-30s" "$topic"
  docker exec kafka1 /opt/kafka/bin/kafka-run-class.sh \
    kafka.tools.GetOffsetShell \
    --broker-list $BROKER \
    --topic $topic \
    --time -1 2>/dev/null | \
    awk -F: '{sum += $3} END {print "total offsets: " sum}'
done

echo ""
echo "⚡ Transaction States"
echo "────────────────────────"
for txid in chat-processor-1 timeline-processor-1; do
  echo "  $txid:"
  docker exec kafka1 /opt/kafka/bin/kafka-transactions.sh \
    --bootstrap-server $BROKER \
    describe \
    --transactional-id $txid 2>/dev/null | grep -E "ProducerEpoch|TransactionState"
done

echo ""
echo "════════════════════════════════════════"