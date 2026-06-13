#!/usr/bin/env python3
"""
Fraud Detection Pipeline — Load Generator

Inserts synthetic payment transactions into Postgres at a configurable rate,
with a tunable percentage of clearly-fraudulent patterns that trigger
the Flink anomaly detection rules.

Usage:
    python src/generator.py --rate 500 --fraud-pct 5 --duration 300

Prometheus metrics are exposed at http://0.0.0.0:8000/metrics.
"""

import sys
import os
import time
import signal
import logging
import threading
from datetime import datetime, timezone

import click
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Make sure sibling modules are importable when run as `python src/generator.py`
sys.path.insert(0, os.path.dirname(__file__))

import db
import fraud_patterns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger("load-generator")

# ── Prometheus metrics ────────────────────────────────────────────────────────
TXNS_INSERTED = Counter(
    "load_gen_transactions_inserted_total",
    "Total transactions inserted into Postgres",
    ["type"],  # normal | fraud
)
INSERT_ERRORS = Counter(
    "load_gen_insert_errors_total",
    "Total insert errors",
)
INSERT_LATENCY = Histogram(
    "load_gen_insert_latency_seconds",
    "Time spent per batch insert",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
CURRENT_RATE = Gauge(
    "load_gen_current_rate_tps",
    "Target transactions per second",
)
TOTAL_INSERTED = Counter(
    "load_gen_total_inserted",
    "Grand total inserted since start",
)


_stop_event = threading.Event()


def _shutdown(sig, frame):
    log.info("Shutdown signal received — draining...")
    _stop_event.set()


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


@click.command()
@click.option("--rate",       default=100,   show_default=True, help="Target transactions per second")
@click.option("--fraud-pct",  default=5.0,   show_default=True, help="Percentage of fraudulent patterns (0–100)")
@click.option("--duration",   default=0,     show_default=True, help="Run duration in seconds (0 = run until killed)")
@click.option("--batch-size", default=50,    show_default=True, help="Inserts per DB round trip")
@click.option("--metrics-port", default=8000, show_default=True, help="Prometheus metrics HTTP port")
def main(rate: int, fraud_pct: float, duration: int, batch_size: int, metrics_port: int):
    """Generate synthetic payment transactions into Postgres."""
    log.info(
        "Starting load generator: rate=%d tps, fraud_pct=%.1f%%, duration=%ds, batch=%d",
        rate, fraud_pct, duration, batch_size,
    )
    CURRENT_RATE.set(rate)

    start_http_server(metrics_port)
    log.info("Prometheus metrics → http://0.0.0.0:%d/metrics", metrics_port)

    # Verify DB connectivity
    try:
        db.get_pool()
        log.info("Postgres connection pool established.")
    except Exception as exc:
        log.error("Cannot connect to Postgres: %s", exc)
        sys.exit(1)

    batch_interval = batch_size / rate  # seconds between batches
    start_time = time.monotonic()
    total = 0

    try:
        while not _stop_event.is_set():
            if duration > 0 and (time.monotonic() - start_time) >= duration:
                log.info("Duration reached. Stopping.")
                break

            t0 = time.monotonic()
            rows = fraud_patterns.all_patterns(fraud_pct, batch_size)

            # Count fraud vs normal for metrics
            fraud_count = sum(
                1 for r in rows
                if r.get("merchant_id") == fraud_patterns.FRAUD_MERCHANT
            )
            normal_count = len(rows) - fraud_count

            try:
                with INSERT_LATENCY.time():
                    inserted = db.insert_transactions(rows)
                TXNS_INSERTED.labels("normal").inc(normal_count)
                TXNS_INSERTED.labels("fraud").inc(fraud_count)
                TOTAL_INSERTED.inc(inserted)
                total += inserted
            except Exception as exc:
                INSERT_ERRORS.inc()
                log.warning("Insert error: %s", exc)

            elapsed = time.monotonic() - t0
            sleep_for = max(0.0, batch_interval - elapsed)
            if sleep_for > 0:
                _stop_event.wait(sleep_for)

    finally:
        db.close_pool()
        elapsed_total = time.monotonic() - start_time
        actual_tps = total / elapsed_total if elapsed_total > 0 else 0
        log.info(
            "Done. Inserted %d transactions in %.1fs (%.0f tps actual)",
            total, elapsed_total, actual_tps,
        )


if __name__ == "__main__":
    main()
