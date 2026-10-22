"""Build and publish the Demo 2 flow: Kafka -> PostgreSQL Metadata -> JDBC Producer.

Usage: python flows/demo2_kafka_drift.py --env dev
Credentials come from environment variables, never from this file.
"""
import argparse
import os

import yaml
from streamsets.sdk import ControlHub

FLOW_NAME = "Demo 2 - Kafka orders events with drift"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()

    with open("flows/params.yaml", encoding="utf-8") as f:
        params = yaml.safe_load(f)[args.env]

    sch = ControlHub(
        credential_id=os.environ["SSET_CRED_ID"],
        token=os.environ["SSET_TOKEN"],
    )

    builder = sch.get_pipeline_builder(
        engine_type="data_collector",
        engine_id=os.environ["SSET_ENGINE_ID"],
    )

    kafka = builder.add_stage("Kafka Multitopic Consumer")
    kafka.set_attributes(
        broker_uri="${KAFKA_BROKER}",
        consumer_group="${CONSUMER_GROUP}",
        topic_list=["${KAFKA_TOPIC}"],
        data_format="JSON",
    )

    metadata = builder.add_stage("PostgreSQL Metadata")
    metadata.set_attributes(
        jdbc_connection_string="${JDBC_URL}",
        schema_name="${TARGET_SCHEMA}",
        table_name="${TARGET_TABLE}",
    )

    target = builder.add_stage("JDBC Producer")
    target.set_attributes(
        jdbc_connection_string="${JDBC_URL}",
        schema_name="${TARGET_SCHEMA}",
        table_name="${TARGET_TABLE}",
    )

    kafka >> metadata >> target

    flow = builder.build(FLOW_NAME)
    flow.add_parameters(**params)
    sch.publish_pipeline(flow, commit_message=f"CI build for {args.env}")
    print(f"Published '{FLOW_NAME}' for {args.env}")


if __name__ == "__main__":
    main()
