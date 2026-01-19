#!/bin/bash

for i in {1..10}
do
    if (( i % 2 == 0 )); then
        key="user_A"
    else
        key="user_B"
    fi
    value="{\"filename\":\"photo_$i.jpg\", \"timestamp\":\"$(date +%s)\"}"

    echo "$key:$value"
    sleep 1.0
done | docker exec -i kafka3 /opt/kafka/bin/kafka-console-producer.sh \
    --topic photo-processing-queue \
    --bootstrap-server kafka3:29092 \
    --property "parse.key=true" \
    --property "key.separator=:"

echo "10 messages have been sent to the photo-processing-queue topic." 