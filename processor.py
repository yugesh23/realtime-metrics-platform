
import logging
import os
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROCESSOR_LOCK_ID = 7619024102

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
"""

CREATE_METRICS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_metric_processed_minute
ON metric_minute_processed (minute DESC);
"""

CREATE_ALERTS_TABLE = """
CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    metric TEXT NOT NULL,
    wiki TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    zscore DOUBLE PRECISION NOT NULL,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'open'
);
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
WHERE event_ts >= NOW() - INTERVAL '10 minutes'
  AND event_ts < date_trunc('minute', NOW())
GROUP BY wiki, date_trunc('minute', event_ts)
ON CONFLICT (wiki, minute) DO UPDATE SET
    edits = EXCLUDED.edits,
    bot_share_pct = EXCLUDED.bot_share_pct,
    avg_abs_edit_size = EXCLUDED.avg_abs_edit_size,
    processed_at = NOW();
"""

ANOMALY_SQL = """
WITH recent AS (
    SELECT wiki, minute, edits
    FROM metric_minute_processed
    WHERE minute >= date_trunc('minute', NOW()) - INTERVAL '2 minutes'
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
     AND h.minute >= r.minute - INTERVAL '60 minutes'
     AND h.minute < r.minute
    GROUP BY r.wiki, r.minute, r.edits
),
candidates AS (
    SELECT
        wiki,
        minute,
        edits,
        (edits - mean_edits) / NULLIF(sd_edits, 0) AS zscore
    FROM baseline
    WHERE sample_count >= 10
      AND sd_edits > 0
      AND ABS((edits - mean_edits) / NULLIF(sd_edits, 0)) >= 3
)
INSERT INTO alerts (metric, wiki, value, zscore, fired_at, status)
SELECT
    'edit_activity',
    c.wiki,
    c.edits::double precision,
    c.zscore,
    NOW(),
    'open'
FROM candidates c
WHERE NOT EXISTS (
    SELECT 1
    FROM alerts a
    WHERE a.metric = 'edit_activity'
      AND a.wiki = c.wiki
      AND a.fired_at >= c.minute
      AND a.fired_at < c.minute + INTERVAL '1 minute'
);
"""


def get_connection():
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError(
            "DB_PASSWORD is missing from your .env file."
        )

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realtime_metrics"),
        user=os.getenv("DB_USER", "postgres"),
        password=password,
        connect_timeout=5,
        application_name="realtime_metrics_processor",
    )


def acquire_single_instance_lock(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_try_advisory_lock(%s);",
            (PROCESSOR_LOCK_ID,),
        )
        acquired = cursor.fetchone()[0]

    connection.commit()

    if not acquired:
        raise RuntimeError(
            "Another processor instance is already running. "
            "Stop the duplicate process before restarting."
        )


def ensure_schema(connection):
    with connection.cursor() as cursor:
        cursor.execute(CREATE_METRICS_TABLE)
        cursor.execute(CREATE_METRICS_INDEX)
        cursor.execute(CREATE_ALERTS_TABLE)

    connection.commit()


def process_once(connection):
    with connection.cursor() as cursor:
        cursor.execute(AGGREGATE_SQL)
        aggregated_rows = cursor.rowcount

        # Serialize anomaly detection within this transaction.
        cursor.execute(
            "SELECT pg_advisory_xact_lock(%s, %s);",
            (761902, 4102),
        )

        cursor.execute(ANOMALY_SQL)
        inserted_alerts = cursor.rowcount

    connection.commit()

    return aggregated_rows, inserted_alerts


def main():
    connection = None
    lock_acquired = False

    try:
        while True:
            try:
                if connection is None or connection.closed:
                    connection = get_connection()
                    acquire_single_instance_lock(connection)
                    lock_acquired = True
                    ensure_schema(connection)

                    logging.info(
                        "Connected; processor lock acquired and schema is ready."
                    )

                aggregated_rows, inserted_alerts = process_once(connection)

                logging.info(
                    "Processing completed. "
                    "Metric rows affected: %s; new alerts: %s",
                    aggregated_rows,
                    inserted_alerts,
                )

                time.sleep(15)

            except (psycopg2.Error, OSError):
                logging.exception("Processor error; reconnecting.")

                if connection is not None:
                    try:
                        connection.rollback()
                        connection.close()
                    except Exception:
                        pass

                connection = None
                lock_acquired = False
                time.sleep(5)

    except RuntimeError:
        logging.exception("Processor could not start.")

    except KeyboardInterrupt:
        logging.info("Stopping processor.")

    except psycopg2.Error:
        logging.exception("Processor could not connect to PostgreSQL.")

    finally:
        if connection is not None and not connection.closed:
            connection.close()

        logging.info("Processor stopped; database connection closed.")


if __name__ == "__main__":
    main()
