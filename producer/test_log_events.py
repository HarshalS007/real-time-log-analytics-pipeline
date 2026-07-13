"""
Lightweight sanity checks for log_events.py. Not a full pytest suite — just
enough to confirm the generator's statistical behavior is sane before
trusting it to feed the pipeline. Run with: python test_log_events.py
"""
from collections import Counter

from log_events import IncidentSimulator, SERVICES, generate_event


def test_basic_shape():
    incidents = IncidentSimulator()
    event = generate_event(incidents, now=1_000_000.0)
    required_keys = {"event_time", "service", "level", "status_code", "latency_ms", "message", "host", "trace_id"}
    assert required_keys.issubset(event.keys()), f"missing keys: {required_keys - event.keys()}"
    assert event["service"] in SERVICES
    assert event["level"] in {"INFO", "WARN", "ERROR"}
    assert 0 <= event["status_code"] < 600
    assert event["latency_ms"] > 0
    print("test_basic_shape: OK")


def test_status_level_consistency():
    incidents = IncidentSimulator()
    for i in range(2000):
        event = generate_event(incidents, now=1_000_000.0 + i)
        if event["status_code"] >= 500:
            assert event["level"] == "ERROR", f"5xx logged as {event['level']}"
        if 400 <= event["status_code"] < 500:
            assert event["level"] in {"WARN", "ERROR"}, f"4xx logged as {event['level']}"
    print("test_status_level_consistency: OK (2000 events)")


def test_incident_raises_error_rate():
    """Error rate should be meaningfully higher while a service is 'incident'."""
    incidents = IncidentSimulator(incident_chance_per_tick=1.0, min_duration=1000, max_duration=1000)
    counts = Counter()
    n = 500
    for i in range(n):
        event = generate_event(incidents, now=2_000_000.0 + i)
        if event["service"] == SERVICES[0]:
            counts[event["level"]] += 1
    total = sum(counts.values())
    if total > 0:
        error_rate = counts["ERROR"] / total
        print(f"test_incident_raises_error_rate: error_rate during forced incident = {error_rate:.2f}")
        assert error_rate > 0.2, "expected elevated error rate during an incident window"
    print("test_incident_raises_error_rate: OK")


def test_rate_distribution_sanity():
    """Over many events, roughly 5 services should each get a non-trivial share."""
    incidents = IncidentSimulator()
    counts = Counter()
    for i in range(5000):
        event = generate_event(incidents, now=3_000_000.0 + i)
        counts[event["service"]] += 1
    assert len(counts) == len(SERVICES)
    for s in SERVICES:
        assert counts[s] > 500, f"{s} under-represented: {counts[s]}"
    print(f"test_rate_distribution_sanity: OK ({dict(counts)})")


if __name__ == "__main__":
    test_basic_shape()
    test_status_level_consistency()
    test_incident_raises_error_rate()
    test_rate_distribution_sanity()
    print("\nAll sanity checks passed.")
