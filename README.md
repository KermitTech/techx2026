# TEC-1291 demo assets

Demo assets for the IBM TechXchange 2026 session TEC-1291:
"IBM StreamSets for watsonx.data Integration: Business Use Cases, Best Practices & Real-World Demos".

## Contents

| Path | Purpose |
|---|---|
| `flows/demo2_kafka_drift.py` | Builds and publishes the Demo 2 flow (Kafka to PostgreSQL with drift handling) with the StreamSets SDK for Python |
| `flows/params.yaml` | Runtime parameters for each environment (dev, test) |
| `.github/workflows/deploy-flow.yml` | CI job: builds the flow and publishes it when `flows/` changes |
| `scripts/setup-demo-stack.sh` | Starts PostgreSQL (CDC), Kafka and the Data Collector engine with Docker |
| `scripts/send-orders.sh` | Sends sample order events, with and without a new field |

## Demo flows

1. Demo 1: PostgreSQL CDC to lakehouse table (`shop.public.orders` to `lakehouse.orders`)
2. Demo 2: Kafka topic `orders.events` to `lakehouse.order_events`, new fields add columns at run time
3. Demo 3: this repository. A change to `flows/params.yaml` triggers the CI job

## CI secrets

The workflow reads these repository secrets. They are not stored in the repository.

- `SSET_CRED_ID`, `SSET_TOKEN`: credentials for the control plane
- `SSET_ENGINE_ID`: target engine ID

## Note

Verify the SDK method names against the SDK version of your environment before you use the script.

Kermit Tech AS
