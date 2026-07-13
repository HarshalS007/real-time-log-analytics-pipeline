-- Real-Time Log Analytics Pipeline: ClickHouse schema
--
-- Ingestion path: Kafka -> logs_queue (Kafka engine, no data stored) ->
-- logs_mv (materialized view) -> logs (MergeTree, actually stores data).
-- ClickHouse's Kafka table engine polls the topic continuously and the
-- materialized view fires per-block, so events land in `logs` within a
-- couple of seconds of being produced — no separate consumer process needed.

CREATE DATABASE IF NOT EXISTS logs_db;

-- 1. Kafka engine "table" - a view over the topic, not physical storage.
CREATE TABLE IF NOT EXISTS logs_db.logs_queue
(
    event_time  DateTime64(3),
    service     String,
    level       String,
    status_code UInt16,
    latency_ms  Float32,
    message     String,
    host        String,
    trace_id    String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'logs',
    kafka_group_name = 'clickhouse_logs_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    date_time_input_format = 'best_effort';

-- 2. Physical storage table. Partitioned by day and ordered for fast
--    per-service, time-ranged queries (exactly what the Grafana panels run).
CREATE TABLE IF NOT EXISTS logs_db.logs
(
    event_time   DateTime64(3),
    ingested_at  DateTime64(3) DEFAULT now64(3),
    service      LowCardinality(String),
    level        LowCardinality(String),
    status_code  UInt16,
    latency_ms   Float32,
    message      String,
    host         String,
    trace_id     String
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (service, event_time)
TTL toDateTime(event_time) + INTERVAL 30 DAY;

-- 3. Materialized view: fires automatically as ClickHouse reads blocks off
--    the Kafka engine table, writing them into the MergeTree table above.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.logs_mv
TO logs_db.logs
AS
SELECT
    event_time,
    now64(3) AS ingested_at,
    service,
    level,
    status_code,
    latency_ms,
    message,
    host,
    trace_id
FROM logs_db.logs_queue;

-- 4. Pre-aggregated per-minute metrics, used directly by the Grafana
--    dashboards: P95 latency and error rate per service over time.
CREATE VIEW IF NOT EXISTS logs_db.logs_metrics_1m AS
SELECT
    toStartOfMinute(event_time)                    AS minute,
    service,
    count()                                        AS total_requests,
    countIf(status_code >= 500)                    AS error_count,
    countIf(status_code >= 500) / count()           AS error_rate,
    quantile(0.95)(latency_ms)                      AS p95_latency_ms,
    quantile(0.50)(latency_ms)                      AS p50_latency_ms
FROM logs_db.logs
GROUP BY minute, service;

-- 5. Convenience view to verify end-to-end ingestion latency
--    (ingested_at - event_time), used in the README's verification step.
CREATE VIEW IF NOT EXISTS logs_db.ingestion_latency AS
SELECT
    event_time,
    ingested_at,
    dateDiff('millisecond', event_time, ingested_at) AS latency_ms
FROM logs_db.logs
ORDER BY event_time DESC
LIMIT 1000;
