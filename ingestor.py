"""
Reliable Event Ingestor for Wikimedia EventStreams.
Streams live Wikipedia edits, validates payloads, batches inserts into PostgreSQL,
maintains single-instance advisory lock, and reports pipeline health.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from db import get_connection, update_health

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ingestor")

STREAM_URL = os.getenv("WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.getenv("USER_AGENT", "RealtimeMetricsMonitoringPlatform/2.0 (student/portfolio project)")
BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "50"))
FLUSH_INTERVAL = float(os.getenv("INGEST_FLUSH_INTERVAL", "1.0"))
HEARTBEAT_INTERVAL = float(os.getenv("INGEST_HEARTBEAT_INTERVAL", "5.0"))

INSERT_SQL = """
INSERT INTO events_raw (
    event_id, wiki, event_type, is_bot, user_name, title,
    event_ts, len_old, len_new, comment
)
VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""

INGESTOR_LOCK_ID = 7619024101


class IngestionStats:
    """Thread-safe telemetry counters for event ingestion."""
    def __init__(self):
        self.received = 0
        self.inserted = 0
        self.skipped = 0
        self.malformed = 0
        self.failed = 0
        self.last_event_ts = None
        self.start_time = time.time()

    def to_dict(self):
        return {
            "received": self.received,
            "inserted": self.inserted,
            "skipped": self.skipped,
            "malformed": self.malformed,
            "failed": self.failed,
            "last_event_ts": self.last_event_ts.isoformat() if self.last_event_ts else None,
            "uptime_seconds": round(time.time() - self.start_time, 1),
        }


def acquire_single_instance_lock(connection):
    """Acquire PostgreSQL session-level advisory lock to ensure single consumer."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s);", (INGESTOR_LOCK_ID,))
        acquired = cursor.fetchone()[0]
    connection.commit()

    if not acquired:
        raise RuntimeError(
            "Another ingestor instance already holds the PostgreSQL advisory lock (ID %s). "
            "Terminate the duplicate process before restarting." % INGESTOR_LOCK_ID
        )


def parse_event(event_dict):
    """
    Validate and extract required fields from raw Wikimedia JSON event.
    Returns a tuple suitable for INSERT, or None if malformed or invalid.
    """
    if not isinstance(event_dict, dict):
        return None

    meta = event_dict.get("meta") or {}
    event_id = meta.get("id")
    timestamp = event_dict.get("timestamp")
    wiki = event_dict.get("wiki")

    # Required field validation
    if not event_id or timestamp is None or not wiki:
        return None

    try:
        event_ts = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None

    lengths = event_dict.get("length") or {}
    len_old = lengths.get("old") if isinstance(lengths.get("old"), int) else None
    len_new = lengths.get("new") if isinstance(lengths.get("new"), int) else None

    # Sanitize and truncate strings to avoid DB bloat or logging sensitive comments
    user_name = event_dict.get("user")
    if user_name and isinstance(user_name, str):
        user_name = user_name[:255]

    title = event_dict.get("title")
    if title and isinstance(title, str):
        title = title[:500]

    comment = event_dict.get("comment")
    if comment and isinstance(comment, str):
        comment = comment[:1000]

    event_type = event_dict.get("type")
    if event_type and isinstance(event_type, str):
        event_type = event_type[:50]

    is_bot = bool(event_dict.get("bot", False))

    return (
        str(event_id),
        str(wiki)[:50],
        event_type,
        is_bot,
        user_name,
        title,
        event_ts,
        len_old,
        len_new,
        comment,
    )


def flush_batch(connection, batch, stats):
    """Flush accumulated event batch to PostgreSQL with transaction safety."""
    if not batch:
        return 0

    try:
        with connection.cursor() as cursor:
            execute_values(cursor, INSERT_SQL, batch, page_size=len(batch))
            inserted_count = cursor.rowcount
        connection.commit()

        # In execute_values with ON CONFLICT DO NOTHING, rowcount reflects inserted rows
        stats.inserted += inserted_count
        stats.skipped += len(batch) - inserted_count
        return inserted_count
    except Exception as exc:
        connection.rollback()
        stats.failed += len(batch)
        logger.error("Failed to insert batch of %d events: %s", len(batch), exc)
        raise


def run_ingestor():
    """Main ingestion loop with reconnection backoff and health updates."""
    import requests

    stats = IngestionStats()
    running = True

    def handle_shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received (signal %s). Finishing up...", signum)
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
    batch = []
    last_flush_time = time.time()
    last_heartbeat_time = 0.0

    # Exponential backoff parameters
    backoff = 1.0
    max_backoff = 60.0

    logger.info("Initializing Real-Time Wikimedia Ingestor...")

    try:
        connection = get_connection("realtime_metrics_ingestor")
        acquire_single_instance_lock(connection)
        logger.info("Database connected. Ingestor advisory lock %s acquired.", INGESTOR_LOCK_ID)
        update_health(connection, "ingestor", "starting", stats.to_dict())

        while running:
            try:
                # Ensure connection is active
                if connection is None or connection.closed:
                    connection = get_connection("realtime_metrics_ingestor")
                    acquire_single_instance_lock(connection)
                    logger.info("Re-acquired DB connection and advisory lock.")

                logger.info("Connecting to Wikimedia EventStreams: %s", STREAM_URL)
                update_health(connection, "ingestor", "connecting", stats.to_dict())

                with requests.get(
                    STREAM_URL,
                    headers={"User-Agent": USER_AGENT},
                    stream=True,
                    timeout=(10, 90),
                ) as response:
                    response.raise_for_status()
                    logger.info("Wikimedia EventStream connected successfully.")
                    backoff = 1.0  # Reset backoff on successful connection

                    update_health(connection, "ingestor", "streaming", stats.to_dict())

                    for line in response.iter_lines(decode_unicode=True):
                        if not running:
                            break

                        now = time.time()

                        # Flush buffer if interval passed or batch is full
                        if batch and (len(batch) >= BATCH_SIZE or (now - last_flush_time) >= FLUSH_INTERVAL):
                            flush_batch(connection, batch, stats)
                            batch.clear()
                            last_flush_time = now

                        # Periodically update health heartbeat
                        if (now - last_heartbeat_time) >= HEARTBEAT_INTERVAL:
                            update_health(connection, "ingestor", "streaming", stats.to_dict())
                            last_heartbeat_time = now
                            if stats.inserted > 0 and stats.inserted % 500 == 0:
                                logger.info(
                                    "Ingestion telemetry: %d received, %d inserted, %d skipped, %d malformed",
                                    stats.received, stats.inserted, stats.skipped, stats.malformed,
                                )

                        if not line:
                            continue
                        if isinstance(line, bytes):
                            line = line.decode("utf-8", errors="replace")
                        if not line.startswith("data:"):
                            continue

                        stats.received += 1
                        raw_data = line[5:].strip()

                        try:
                            event_data = json.loads(raw_data)
                        except json.JSONDecodeError:
                            stats.malformed += 1
                            continue

                        row = parse_event(event_data)
                        if row is None:
                            stats.skipped += 1
                            continue

                        stats.last_event_ts = row[6]  # event_ts
                        batch.append(row)

            except (requests.RequestException, OSError) as exc:
                if not running:
                    break

                # Flush remaining buffer before reconnecting
                if connection and not connection.closed and batch:
                    try:
                        flush_batch(connection, batch, stats)
                        batch.clear()
                    except Exception:
                        pass

                jitter = random.uniform(0.8, 1.2)
                sleep_duration = min(backoff * jitter, max_backoff)
                logger.warning(
                    "Stream error: %s. Reconnecting in %.1f seconds (backoff: %.1fs)...",
                    exc, sleep_duration, backoff
                )
                if connection and not connection.closed:
                    try:
                        update_health(connection, "ingestor", "reconnecting", stats.to_dict())
                    except Exception:
                        pass

                time.sleep(sleep_duration)
                backoff = min(backoff * 2, max_backoff)

            except psycopg2.Error as db_err:
                if not running:
                    break
                logger.error("PostgreSQL error in ingestor loop: %s. Reconnecting in 5s...", db_err)
                if connection:
                    try:
                        connection.rollback()
                        connection.close()
                    except Exception:
                        pass
                connection = None
                time.sleep(5)

        # Graceful exit
        logger.info("Ingestor loop stopped. Flushing final batch of %d events...", len(batch))
        if connection and not connection.closed:
            if batch:
                try:
                    flush_batch(connection, batch, stats)
                    batch.clear()
                except Exception as exc:
                    logger.error("Failed flushing final batch: %s", exc)

            update_health(connection, "ingestor", "stopped", stats.to_dict())

    except RuntimeError as r_err:
        logger.error("Ingestor fatal startup error: %s", r_err)
    except Exception as exc:
        logger.exception("Unexpected exception in ingestor: %s", exc)
    finally:
        if connection and not connection.closed:
            try:
                update_health(connection, "ingestor", "stopped", stats.to_dict())
                connection.close()
            except Exception:
                pass
        logger.info(
            "Ingestor terminated cleanly. Summary: %d received, %d inserted, %d skipped, %d failed.",
            stats.received, stats.inserted, stats.skipped, stats.failed
        )


if __name__ == "__main__":
    run_ingestor()
