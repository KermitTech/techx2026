#!/usr/bin/env bash
# TEC-1291 demo stack: PostgreSQL 16 with CDC (wal2json), Kafka (KRaft), StreamSets Data Collector engine.
#
# Prerequisites (export before you run):
#   SSET_API_KEY        IBM Cloud API key
#   SSET_PROJECT_ID     watsonx.data integration project ID
#   SSET_ENVIRONMENT_ID StreamSets environment ID
#   SSET_BASE_URL       for example https://api.ca-tor.dai.cloud.ibm.com
#   PG_PASSWORD         password for the demo database user
# Optional: put presto-jdbc-<version>.jar in ./sdc-extras/streamsets-datacollector-jdbc-lib/lib/
set -euo pipefail

: "${SSET_API_KEY:?}" "${SSET_PROJECT_ID:?}" "${SSET_ENVIRONMENT_ID:?}" "${SSET_BASE_URL:?}" "${PG_PASSWORD:?}"

NET=techxchange-net
docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET"

# 1. PostgreSQL with wal2json
mkdir -p pg-cdc && cat > pg-cdc/Dockerfile <<'EOF'
FROM postgres:16
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-16-wal2json && rm -rf /var/lib/apt/lists/*
EOF
docker build -t techxchange/postgres-cdc:16 pg-cdc

docker run -d --name tx-postgres --network "$NET" -p 5432:5432 \
  -e POSTGRES_USER=demo -e POSTGRES_PASSWORD="$PG_PASSWORD" -e POSTGRES_DB=shop \
  techxchange/postgres-cdc:16 \
  -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4

until docker exec tx-postgres pg_isready -U demo -d shop >/dev/null 2>&1; do sleep 2; done

# Allow wal2json as an output plugin (one quoted value per library)
docker exec tx-postgres psql -U demo -d shop \
  -c "ALTER SYSTEM SET output_plugin_libraries = 'pgoutput', 'test_decoding', 'wal2json';"
docker restart tx-postgres
until docker exec tx-postgres pg_isready -U demo -d shop >/dev/null 2>&1; do sleep 2; done

docker exec -i tx-postgres psql -U demo -d shop <<'EOF'
ALTER USER demo WITH REPLICATION;
CREATE TABLE IF NOT EXISTS public.orders (
  order_id SERIAL PRIMARY KEY, customer_id INT NOT NULL, country TEXT NOT NULL,
  amount NUMERIC(10,2) NOT NULL, status TEXT NOT NULL DEFAULT 'NEW',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
ALTER TABLE public.orders REPLICA IDENTITY FULL;
INSERT INTO public.orders (customer_id, country, amount, status) VALUES
  (1001,'NO',249.00,'NEW'),(1002,'SE',89.50,'NEW'),(1003,'DK',1200.00,'PAID'),
  (1004,'GB',35.99,'SHIPPED'),(1005,'AE',560.00,'NEW');

CREATE SCHEMA IF NOT EXISTS lakehouse;
-- Demo 1 target, loaded with the current rows
CREATE TABLE IF NOT EXISTS lakehouse.orders (
  order_id INT PRIMARY KEY, customer_id INT, country TEXT, amount NUMERIC(10,2),
  status TEXT, updated_at TIMESTAMPTZ, ingested_at TIMESTAMPTZ);
INSERT INTO lakehouse.orders SELECT order_id, customer_id, country, amount, status, updated_at, now()
  FROM public.orders ON CONFLICT (order_id) DO NOTHING;
-- Demo 2 target. JSON numbers arrive as DOUBLE
CREATE TABLE IF NOT EXISTS lakehouse.order_events (
  order_id INT, country TEXT, amount DOUBLE PRECISION, status TEXT);
EOF

# 2. Kafka (KRaft) with the Demo 2 topic
docker run -d --name tx-kafka --network "$NET" -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://tx-kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:3.8.0
sleep 15
docker exec tx-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic orders.events --partitions 1 --replication-factor 1

# 3. StreamSets Data Collector engine. Extra JDBC drivers go in the extras folder, not in user-libs
mkdir -p sdc-extras/streamsets-datacollector-jdbc-lib/lib && chmod -R a+rX sdc-extras
docker run -d --name tx-sdc --network "$NET" --cpus 4.0 \
  -v "$PWD/sdc-extras:/opt/sdc-extras:ro" \
  -e STREAMSETS_LIBRARIES_EXTRA_DIR=/opt/sdc-extras \
  -e SSET_PROJECT_ID="$SSET_PROJECT_ID" \
  -e SSET_ENVIRONMENT_ID="$SSET_ENVIRONMENT_ID" \
  -e SSET_BASE_URL="$SSET_BASE_URL" \
  -e SSET_API_KEY="$SSET_API_KEY" \
  icr.io/streamsets/datacollector:7.9.0

echo "Done. Engine log: docker logs -f tx-sdc"
