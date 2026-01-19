#!/bin/bash

for i in {1..30}
do
    if (( i % 2 == 0 )); then
        key="user_A"
    else
        key="user_B"
    fi
    value="$key: \"user performed a task\""

    echo "$value"
    sleep 0.500
done | docker exec -i kafka3 /opt/kafka/bin/kafka-console-producer.sh \
    --topic user-clicks \
    --bootstrap-server kafka3:29092 \
    --property "parse.key=true" \
    --property "key.separator=:"

echo "10 messages have been sent to the user-clicks topic." 