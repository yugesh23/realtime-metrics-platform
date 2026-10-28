"""
Safe, repeatable, non-destructive migration script for Real-Time Metrics Platform.
Preserves all historical records, creates historical backup, upgrades alert schema,
and establishes health tracking.
"""

import logging
import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=r"E:\realtime-metrics-platform\.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def get_connection():
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("DB_PASSWORD is missing from .env")

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realtime_metrics"),
        user=os.getenv("DB_USER", "postgres"),
        password=password,
        connect_timeout=5,
        application_name="migration_script",
    )


def run_migration():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logging.info("Step 1: Creating backup table of alerts if not already existing...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts_historical_backup AS
                SELECT * FROM alerts;
            """)

            cur.execute("SELECT count(*) FROM alerts_historical_backup;")
            backup_count = cur.fetchone()[0]
            logging.info("Historical backup confirmed with %s records.", backup_count)

            logging.info("Step 2: Upgrading alerts table columns safely...")
            cur.execute("""
                ALTER TABLE alerts
                    ADD COLUMN IF NOT EXISTS source_minute TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS baseline_mean DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS baseline_std DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'medium',
                    ADD COLUMN IF NOT EXISTS first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ADD COLUMN IF NOT EXISTS incident_count INTEGER NOT NULL DEFAULT 1,
                    ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS acknowledged_by TEXT,
                    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS resolution_note TEXT,
                    ADD COLUMN IF NOT EXISTS is_historical_duplicate BOOLEAN NOT NULL DEFAULT FALSE;
            """)

            logging.info("Step 3: Populating source_minute and lifecycle timestamps for existing alerts...")
            cur.execute("""
                UPDATE alerts
                SET source_minute = date_trunc('minute', fired_at),
                    first_detected_at = fired_at,
                    last_observed_at = fired_at
                WHERE source_minute IS NULL;
            """)

            logging.info("Step 4: Tagging historical duplicate alerts safely without deleting any record...")
            # For records with same (metric, wiki, source_minute), retain the lowest id as primary,
            # mark duplicate instances as is_historical_duplicate = TRUE.
            cur.execute("""
                WITH duplicates AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY metric, wiki, source_minute
                               ORDER BY id ASC
                           ) AS rn
                    FROM alerts
                )
                UPDATE alerts
                SET is_historical_duplicate = TRUE
                FROM duplicates
                WHERE alerts.id = duplicates.id
                  AND duplicates.rn > 1;
            """)

            cur.execute("SELECT count(*) FROM alerts WHERE is_historical_duplicate = TRUE;")
            dup_count = cur.fetchone()[0]
            logging.info("Tagged %s historical duplicate alert records.", dup_count)

            logging.info("Step 5: Enforcing unique partial index on active alerts (metric, wiki, source_minute)...")
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_unique_identity
                ON alerts (metric, wiki, source_minute)
                WHERE is_historical_duplicate = FALSE;
            """)

            logging.info("Step 6: Creating additional performance indexes on alerts...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status);
                CREATE INDEX IF NOT EXISTS idx_alerts_source_minute ON alerts (source_minute DESC);
                CREATE INDEX IF NOT EXISTS idx_alerts_wiki ON alerts (wiki);
                CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity);
            """)

            logging.info("Step 7: Creating pipeline_health table for real-time status and heartbeats...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_health (
                    component TEXT PRIMARY KEY,
                    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    status TEXT NOT NULL DEFAULT 'healthy',
                    details JSONB NOT NULL DEFAULT '{}'::jsonb
                );
            """)

            logging.info("Step 8: Ensuring optimal indexes on events_raw and metric_minute_processed...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_raw_ts_desc ON events_raw (event_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_metric_minute_wiki_minute ON metric_minute_processed (wiki, minute DESC);
            """)

        conn.commit()
        logging.info("Migration completed successfully with 100% data preservation.")

    except Exception:
        conn.rollback()
        logging.exception("Migration failed; transaction rolled back.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_migration()
