#!/usr/bin/env python3
"""
Generates synthetic log events at a target rate (events/sec) and pushes them
to Kafka (or, with --dry-run, prints them to stdout). Defaults to ~850/sec,
i.e. ~51,000 events/min, matching the pipeline's "50K+ events/min" target.

Usage:
    python producer.py --bootstrap-servers localhost:9092 --topic logs
    python producer.py --dry-run --rate 50 --duration 5
"""
import argparse
import time

from log_events import IncidentSimulator, generate_event
from sinks import KafkaSink, StdoutSink


def run(sink, rate_per_sec, duration_sec, report_every=5.0):
    incidents = IncidentSimulator()
    start = time.time()
    sent = 0
    last_report = start
    tick_interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0

    try:
        while duration_sec == 0 or (time.time() - start) < duration_sec:
            tick_start = time.time()
            event = generate_event(incidents, now=tick_start)
            sink.send(event)
            sent += 1

            now = time.time()
            if now - last_report >= report_every:
                elapsed = now - start
                print(f"[{elapsed:6.1f}s] sent={sent:>7} avg_rate={sent / elapsed:6.1f}/s")
                last_report = now

            # Pace to the target rate rather than firing as fast as possible.
            elapsed_tick = time.time() - tick_start
            sleep_for = tick_interval - elapsed_tick
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nStopping (Ctrl+C)...")
    finally:
        sink.flush()
        sink.close()

    total_elapsed = time.time() - start
    print(f"Done. Sent {sent} events in {total_elapsed:.1f}s (avg {sent / max(total_elapsed, 0.001):.1f}/s).")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="logs", help="Kafka topic to publish to")
    parser.add_argument("--rate", type=float, default=850.0, help="Target events per second (default ~50K/min)")
    parser.add_argument("--duration", type=float, default=0, help="Seconds to run; 0 = run until Ctrl+C")
    parser.add_argument("--dry-run", action="store_true", help="Print events to stdout instead of sending to Kafka")
    args = parser.parse_args()

    if args.dry_run:
        sink = StdoutSink()
    else:
        sink = KafkaSink(bootstrap_servers=args.bootstrap_servers, topic=args.topic)

    print(f"Starting producer: rate={args.rate}/s duration={'infinite' if args.duration == 0 else args.duration}s "
          f"sink={'stdout (dry-run)' if args.dry_run else f'kafka@{args.bootstrap_servers} topic={args.topic}'}")
    run(sink, rate_per_sec=args.rate, duration_sec=args.duration)


if __name__ == "__main__":
    main()
