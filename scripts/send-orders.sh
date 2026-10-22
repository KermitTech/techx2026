#!/usr/bin/env bash
# Usage: ./send-orders.sh base   (original schema)
#        ./send-orders.sh drift  (adds the field "channel")
MODE=${1:-base}
for i in 1 2 3; do
  ID=$((RANDOM % 9000 + 1000))
  if [ "$MODE" = "drift" ]; then
    echo "{\"order_id\":$ID,\"country\":\"NO\",\"amount\":$((RANDOM % 900 + 50)).00,\"status\":\"NEW\",\"channel\":\"mobile\"}"
  else
    echo "{\"order_id\":$ID,\"country\":\"SE\",\"amount\":$((RANDOM % 900 + 50)).00,\"status\":\"NEW\"}"
  fi
done | docker exec -i tx-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders.events
echo "Sent 3 $MODE events"
