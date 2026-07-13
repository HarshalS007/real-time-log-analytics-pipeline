"""
Output sinks for generated log events. KafkaSink is what's used in
production; the other two exist so the generator's logic (rate, event
shape, incident simulation) can be exercised and tested without a running
Kafka broker.
"""
import json
import sys


class KafkaSink:
    def __init__(self, bootstrap_servers, topic):
        from kafka import KafkaProducer  # imported lazily so dry-run modes don't need kafka-python's broker deps

        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=20,       # small batching window for throughput without hurting the <2s latency target
            batch_size=32768,
            acks=1,
        )

    def send(self, event):
        self.producer.send(self.topic, value=event)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()


class StdoutSink:
    """Prints each event as JSON. Useful for `--dry-run` sanity checks."""

    def __init__(self, stream=None):
        self.stream = stream or sys.stdout

    def send(self, event):
        self.stream.write(json.dumps(event) + "\n")

    def flush(self):
        self.stream.flush()

    def close(self):
        self.flush()


class StatsSink:
    """Records events in memory instead of sending anywhere. Used by tests."""

    def __init__(self):
        self.events = []

    def send(self, event):
        self.events.append(event)

    def flush(self):
        pass

    def close(self):
        pass
