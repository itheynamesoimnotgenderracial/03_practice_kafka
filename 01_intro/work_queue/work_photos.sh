#!/bin/bash

for i in {1..10}
do
    key="photo_id_$i"
    value="{\"filename\":\"photo_$i.jpg\", \"timestamp\":\"$(date +%s)\"}"

    echo "$key:$value"
    sleep 1.0
done | docker exec -i kafka3 /opt/kafka/bin/kafka-console-producer.sh \
    --topic photo-editing-queue \
    --bootstrap-server kafka3:29092 \
    # --property "parse.key=true" \
    # --property "key.separator=:"

echo "10 messages have been sent to the photo-editing-queue topic." 