# Architecture Overview

This repository implements a lightweight real-time log analytics pipeline that matches the project description:

1. Synthetic services emit application logs at a high rate.
2. A Python producer publishes those events to a Kafka topic.
3. ClickHouse reads from the Kafka topic through its native Kafka table engine.
4. Materialized views move the ingested rows into a durable MergeTree table.
5. Grafana visualizes the resulting metrics and logs for latency, errors, and throughput.

The key design choice is that ClickHouse handles the ingestion path directly without a separate consumer service. That keeps the stack simpler while still demonstrating a realistic streaming architecture.
