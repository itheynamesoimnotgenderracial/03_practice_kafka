#!/bin/bash

SCHEMA='{
  "type":"record",
  "name":"ChatTimelineEvent",
  "namespace":"chat",
  "fields":[
    {"name":"room_id","type":"string"},
    {"name":"sequence","type":"long"},
    {"name":"user_id","type":"string"},
    {"name":"message_id","type":"string"},
    {"name":"content","type":"string"},
    {"name":"timestamp","type":"long"}
  ]
}'

for i in {1..10}
do
    random_str=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c6)
    timestamp_ms=$(date +%s%3N)
    random_sequence=$(date +%s%3N)
    user_id=("user-1" "user-2" "user-3")

    if (( i % 2 == 0 )); then
        current_user="${user_id[0]}"
    elif (( i % 2 == 1 )); then
        current_user="${user_id[1]}"
    else
        current_user="${user_id[2]}"
    fi

    message_id=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c3)
    content=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c15)

    value=$(cat <<EOF
{"room_id":"${random_str}","sequence":${random_sequence},"user_id":"${current_user}","message_id":"${message_id}","content":"${content}","timestamp":${timestamp_ms}}
EOF
)
    
    echo "$value"
    sleep 1.0
done | docker exec -i kafka1 /opt/confluent/bin/kafka-avro-console-producer \
    --broker-list kafka1:29092 \
    --topic chat.timeline \
    --property schema.registry.url=http://schema-registry:8081 \
    --property value.schema="$SCHEMA"

echo "10 messages have been sent to the chat.timeline topic." 