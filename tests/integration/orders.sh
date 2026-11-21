#!/usr/bin/env bash
set -euo pipefail
dc=(docker compose)
api=${ORDERFLOW_API:-http://localhost:8080}/api/v1/orders
"${dc[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic order-events --partitions 1 --replication-factor 1
id=$(curl -fsS "$api" -H 'Content-Type: application/json' -d '{"amount":"7.50"}' | jq -er .id)
curl -fsS "$api/$id" | jq -e '.status == "pending"' >/dev/null
"${dc[@]}" exec -T redis redis-cli GET "order:$id" | jq -e --arg id "$id" '.id == $id' >/dev/null
sql() { "${dc[@]}" exec -T postgres psql -U orderflow -d orderflow -Atc "$1"; }
[[ $(sql "select amount from orders where id='$id'") == 7.50 ]]
for attempt in {1..30}; do
  [[ $(sql "select count(*) from order_outbox where order_id='$id' and published_at is not null") == 1 ]] && break
  sleep 1
done
[[ $(sql "select count(*) from order_outbox where order_id='$id' and published_at is not null") == 1 ]]
messages=$("${dc[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic order-events --from-beginning --timeout-ms 5000)
jq -es --arg id "$id" 'any(.[]; .id == $id and .status == "pending")' <<< "$messages"
key="integration-$id"
post(){ curl -fsS "$api" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d '{"amount":"8.50"}'; }
first=$(post | jq -er .id)
[[ $(post | jq -er .id) == "$first" ]]
[[ $(sql "select count(*) from order_outbox where order_id='$first'") == 1 ]]
[[ $(curl -s -o /dev/null -w '%{http_code}' "$api" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d '{"amount":"9"}') == 409 ]]
