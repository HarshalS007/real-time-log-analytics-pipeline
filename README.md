# Log Pipeline Demo

This project is a small end-to-end log analytics demo. It generates synthetic application logs, streams them through Kafka, stores them in ClickHouse, and visualizes them in Grafana.

The goal is simple: show how a modern observability pipeline can look without needing a full production stack or cloud setup. It is a practical demo for learning, portfolio work, or local experimentation.

## What it does

- Generates realistic log events from several mock services
- Simulates short incident windows with higher latency and error rates
- Sends events into Kafka
- Stores and queries data in ClickHouse
- Displays basic service health metrics in Grafana

## Project structure

- producer/ — Python generator and sink logic
- clickhouse/init/01_create_tables.sql — ClickHouse schema and views
- grafana/ — datasource provisioning and dashboard definition
- docker-compose.yml — starts Kafka, ClickHouse, Grafana, and the producer together

## Run it locally

You need Docker and Docker Compose installed.

```bash
docker compose up --build
```

This will bring up the full stack in order:

1. Kafka
2. ClickHouse
3. Grafana
4. The producer that starts sending synthetic logs

Then open:

- Grafana: http://localhost:3000
  - username: admin
  - password: admin
- ClickHouse HTTP console: http://localhost:8123/play

## Quick checks

You can confirm that data is flowing by querying ClickHouse:

```bash
curl -s 'http://localhost:8123/?query=SELECT+count()+FROM+logs_db.logs+WHERE+event_time+>=+now()+-+INTERVAL+1+MINUTE'
```

And for a quick latency view:

```bash
curl -s 'http://localhost:8123/?query=SELECT+quantile(0.50)(latency_ms),+quantile(0.95)(latency_ms),+max(latency_ms)+FROM+logs_db.ingestion_latency+FORMAT+TSV'
```

## Run the producer on its own

If you want to test the log generator without Docker:

```bash
cd producer
pip install -r requirements.txt
python producer.py --dry-run --rate 50 --duration 10
python test_log_events.py
```

## Notes

This is a polished demo project rather than a production-grade observability platform. It is a strong fit for GitHub because it is self-contained, runnable locally, and easy to understand.

If you want, the next step could be adding a small CI workflow, a Makefile, or a sample deployment setup for a cloud environment.
