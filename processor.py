"""
Real-Time Metrics Processor & Anomaly Detection Worker.
Aggregates minute-level edit activity, calculates rolling statistical baselines,
detects significant anomalies, and guarantees idempotent alert deduplication.
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv

from db import get_connection, update_health

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("processor")

PROCESSOR_LOCK_ID = 7619024102

# Configurable parameters via environment variables
CYCLE_INTERVAL_SECONDS = int(os.getenv("PROCESSOR_INTERVAL_SECONDS", "15"))
AGGREGATION_WINDOW_MINUTES = int(os.getenv("AGGREGATION_WINDOW_MINUTES", "30"))
ANOMALY_LOOKBACK_MINUTES = int(os.getenv("ANOMALY_LOOKBACK_MINUTES", "60"))
MIN_SAMPLE_COUNT = int(os.getenv("MIN_SAMPLE_COUNT", "10"))
MIN_EDITS_THRESHOLD = int(os.getenv("MIN_EDITS_THRESHOLD", "5"))
ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "3.0"))

CREATE_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS metric_minute_processed (
    wiki TEXT NOT NULL,
    minute TIMESTAMPTZ NOT NULL,
    edits BIGINT NOT NULL,
    bot_share_pct NUMERIC(6, 2),
    avg_abs_edit_size NUMERIC(14, 2),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (wiki, minute)
);
CREATE INDEX IF NOT EXISTS idx_metric_processed_minute
ON metric_minute_processed (minute DESC);
"""

CREATE_ALERTS_TABLE = """
CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    metric TEXT NOT NULL,
    wiki TEXT NOT NULL,
    source_minute TIMESTAMPTZ,
    value DOUBLE PRECISION NOT NULL,
    baseline_mean DOUBLE PRECISION,
    baseline_std DOUBLE PRECISION,
    zscore DOUBLE PRECISION NOT NULL,
    severity TEXT NOT NULL DEFAULT 'medium',
    first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'open',
    incident_count INTEGER NOT NULL DEFAULT 1,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    resolved_at TIMESTAMPTZ,
    resolution_note TEXT,
    is_historical_duplicate BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_unique_identity
ON alerts (metric, wiki, source_minute)
WHERE is_historical_duplicate = FALSE;
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status);
CREATE INDEX IF NOT EXISTS idx_alerts_source_minute ON alerts (source_minute DESC);
"""

AGGREGATE_SQL = """
INSERT INTO metric_minute_processed (
    wiki,
    minute,
    edits,
    bot_share_pct,
    avg_abs_edit_size,
    processed_at
)
SELECT
    wiki,
    date_trunc('minute', event_ts) AS minute,
    COUNT(*) AS edits,
    ROUND(
        (
            100.0 * COUNT(*) FILTER (WHERE is_bot)
            / NULLIF(COUNT(*), 0)
        )::numeric,
        2
    ) AS bot_share_pct,
    ROUND(
        AVG(ABS(len_new - len_old))::numeric,
        2
    ) AS avg_abs_edit_size,
    NOW()
FROM events_raw
WHERE event_ts >= NOW() - (%s * INTERVAL '1 minute')
  AND event_ts < date_trunc('minute', NOW())
GROUP BY wiki, date_trunc('minute', event_ts)
ON CONFLICT (wiki, minute) DO UPDATE SET
    edits = EXCLUDED.edits,
    bot_share_pct = EXCLUDED.bot_share_pct,
    avg_abs_edit_size = EXCLUDED.avg_abs_edit_size,
    processed_at = NOW();
"""

# Statistical anomaly detection query with robust deduplication via ON CONFLICT
ANOMALY_UPSERT_SQL = """
WITH recent AS (
    SELECT wiki, minute, edits
    FROM metric_minute_processed
    WHERE minute >= date_trunc('minute', NOW()) - INTERVAL '5 minutes'
      AND minute < date_trunc('minute', NOW())
),
baseline AS (
    SELECT
        r.wiki,
        r.minute,
        r.edits,
        AVG(h.edits::double precision) AS mean_edits,
        STDDEV_POP(h.edits::double precision) AS sd_edits,
        COUNT(h.edits) AS sample_count
    FROM recent r
    JOIN metric_minute_processed h
      ON h.wiki = r.wiki
     AND h.minute >= r.minute - (%s * INTERVAL '1 minute')
     AND h.minute < r.minute
    GROUP BY r.wiki, r.minute, r.edits
),
candidates AS (
    SELECT
        wiki,
        minute,
        edits,
        mean_edits,
        sd_edits,
        (edits - mean_edits) / NULLIF(sd_edits, 0) AS zscore,
        CASE
            WHEN ABS((edits - mean_edits) / NULLIF(sd_edits, 0)) >= 6.0 THEN 'critical'
            WHEN ABS((edits - mean_edits) / NULLIF(sd_edits, 0)) >= 4.0 THEN 'high'
            ELSE 'medium'
        END AS severity
    FROM baseline
    WHERE sample_count >= %s
      AND sd_edits > 0
      AND edits >= %s
      AND ABS((edits - mean_edits) / NULLIF(sd_edits, 0)) >= %s
)
INSERT INTO alerts (
    metric,
    wiki,
    source_minute,
    value,
    baseline_mean,
    baseline_std,
    zscore,
    severity,
    first_detected_at,
    last_observed_at,
    fired_at,
    status,
    incident_count
)
SELECT
    'edit_activity',
    c.wiki,
    c.minute,
    c.edits::double precision,
    ROUND(c.mean_edits::numeric, 2)::double precision,
    ROUND(c.sd_edits::numeric, 2)::double precision,
    ROUND(c.zscore::numeric, 2)::double precision,
    c.severity,
    NOW(),
    NOW(),
    NOW(),
    'open',
    1
FROM candidates c
ON CONFLICT (metric, wiki, source_minute) WHERE is_historical_duplicate = FALSE
DO UPDATE SET
    value = EXCLUDED.value,
    zscore = EXCLUDED.zscore,
    baseline_mean = EXCLUDED.baseline_mean,
    baseline_std = EXCLUDED.baseline_std,
    severity = EXCLUDED.severity,
    last_observed_at = NOW(),
    incident_count = alerts.incident_count + 1;
"""


def acquire_single_instance_lock(connection):
    """Acquire session-level advisory lock to prevent multiple processor workers."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s);", (PROCESSOR_LOCK_ID,))
        acquired = cursor.fetchone()[0]
    connection.commit()

    if not acquired:
        raise RuntimeError(
            "Another processor instance is already active (advisory lock %s). "
            "Stop duplicate instances before running." % PROCESSOR_LOCK_ID
        )


def ensure_schema(connection):
    """Ensure required tables, indexes, and constraints exist idempotently."""
    with connection.cursor() as cursor:
        cursor.execute(CREATE_METRICS_TABLE)
        cursor.execute(CREATE_ALERTS_TABLE)
    connection.commit()


def process_cycle(connection):
    """Execute one aggregation and anomaly detection cycle transactionally."""
    start_time = time.time()
    with connection.cursor() as cursor:
        # Step 1: Metric aggregation
        cursor.execute(AGGREGATE_SQL, (AGGREGATION_WINDOW_MINUTES,))
        aggregated_rows = cursor.rowcount

        # Step 2: Anomaly detection with transaction lock
        cursor.execute("SELECT pg_advisory_xact_lock(%s, %s);", (761902, 4102))
        cursor.execute(
            ANOMALY_UPSERT_SQL,
            (
                ANOMALY_LOOKBACK_MINUTES,
                MIN_SAMPLE_COUNT,
                MIN_EDITS_THRESHOLD,
                ZSCORE_THRESHOLD,
            ),
        )
        alert_rows = cursor.rowcount

        # Retrieve latest processed minute
        cursor.execute("SELECT MAX(minute) FROM metric_minute_processed;")
        latest_minute = cursor.fetchone()[0]

    connection.commit()
    duration = time.time() - start_time
    return aggregated_rows, alert_rows, latest_minute, duration


def run_processor():
    """Main processor loop with heartbeat and error recovery."""
    running = True

    def handle_shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received. Exiting processor loop...")
        running = False

    import threading
    if threading.current_thread() is threading.main_thread():
        try:
            signal.signal(signal.SIGINT, handle_shutdown)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, handle_shutdown)
        except (ValueError, AttributeError):
            pass

    connection = None
    logger.info("Starting Real-Time Metrics Processor & Anomaly Engine...")

    try:
        connection = get_connection("realtime_metrics_processor")
        acquire_single_instance_lock(connection)
        ensure_schema(connection)
        logger.info("PostgreSQL connected; advisory lock %s acquired.", PROCESSOR_LOCK_ID)

        while running:
            try:
                if connection is None or connection.closed:
                    connection = get_connection("realtime_metrics_processor")
                    acquire_single_instance_lock(connection)
                    ensure_schema(connection)
                    logger.info("Re-established processor DB connection.")

                aggregated, alerts_count, latest_minute, duration = process_cycle(connection)

                telemetry = {
                    "last_processed_minute": latest_minute.isoformat() if latest_minute else None,
                    "aggregated_rows_last_cycle": aggregated,
                    "alerts_upserted_last_cycle": alerts_count,
                    "cycle_duration_seconds": round(duration, 3),
                    "lookback_window_minutes": ANOMALY_LOOKBACK_MINUTES,
                    "zscore_threshold": ZSCORE_THRESHOLD,
                    "min_edits_threshold": MIN_EDITS_THRESHOLD,
                }
                update_health(connection, "processor", "active", telemetry)

                logger.info(
                    "Cycle complete in %.2fs: %d metric rows updated, %d alerts processed. Latest min: %s",
                    duration, aggregated, alerts_count, latest_minute
                )

                # Sleep until next cycle while checking shutdown flag
                sleep_start = time.time()
                while running and (time.time() - sleep_start) < CYCLE_INTERVAL_SECONDS:
                    time.sleep(1)

            except (psycopg2.Error, OSError) as exc:
                if not running:
                    break
                logger.error("Database error during processor cycle: %s. Reconnecting in 5s...", exc)
                if connection:
                    try:
                        connection.rollback()
                        connection.close()
                    except Exception:
                        pass
                connection = None
                time.sleep(5)

    except RuntimeError as r_err:
        logger.error("Processor startup failure: %s", r_err)
    except Exception as exc:
        logger.exception("Unexpected exception in processor: %s", exc)
    finally:
        if connection and not connection.closed:
            try:
                update_health(connection, "processor", "stopped")
                connection.close()
            except Exception:
                pass
        logger.info("Processor terminated cleanly.")


if __name__ == "__main__":
    run_processor()
