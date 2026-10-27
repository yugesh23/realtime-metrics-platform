import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

INSERT_SQL = """
INSERT INTO events_raw (
    event_id, wiki, event_type, is_bot, user_name, title,
    event_ts, len_old, len_new, comment
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING;
"""

# PostgreSQL advisory lock key used to ensure only one ingestor is active.
INGESTOR_LOCK_ID = 7619024101


def get_connection():
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("DB_PASSWORD is not set in the .env file.")

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realtime_metrics"),
        user=os.getenv("DB_USER", "postgres"),
        password=password,
        connect_timeout=5,
        application_name="realtime_metrics_ingestor",
    )


def acquire_single_instance_lock(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s);", (INGESTOR_LOCK_ID,))
        acquired = cursor.fetchone()[0]
    connection.commit()

    if not acquired:
        raise RuntimeError(
            "Another ingestor instance already holds the PostgreSQL advisory lock. "
            "Stop the duplicate process before starting this one."
        )


def parse_event(event):
    event_id = event.get("meta", {}).get("id")
    timestamp = event.get("timestamp")
    wiki = event.get("wiki")

    if event_id is None or timestamp is None or not wiki:
        return None

    try:
        event_ts = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None

    lengths = event.get("length") or {}

    return (
        str(event_id),
        str(wiki),
        event.get("type"),
        bool(event.get("bot", False)),
        event.get("user"),
        event.get("title"),
        event_ts,
        lengths.get("old"),
        lengths.get("new"),
        event.get("comment"),
    )


def main():
    connection = None
    inserted = 0

    try:
        connection = get_connection()
        acquire_single_instance_lock(connection)
        logging.info("Connected to PostgreSQL; ingestor lock acquired.")

        while True:
            try:
                if connection is None or connection.closed:
                    connection = get_connection()
                    acquire_single_instance_lock(connection)
                    logging.info("PostgreSQL reconnected; ingestor lock acquired.")

                # A stream disconnect should reconnect without losing the DB lock.
                logging.info("Connecting to Wikimedia EventStreams...")

                with requests.get(
                    STREAM_URL,
                    headers={
                        "User-Agent": (
                            "RealtimeMetricsStudentProject/1.0 "
                            "(local development)"
                        )
                    },
                    stream=True,
                    timeout=(10, 90),
                ) as response:
                    response.raise_for_status()
                    logging.info("Wikimedia stream connected.")

                    for line in response.iter_lines(decode_unicode=True):
                        if not line:
                            continue
                        if isinstance(line, bytes):
                            line = line.decode("utf-8", errors="replace")
                        if not line.startswith("data:"):
                            continue

                        try:
                            event = json.loads(line[5:].strip())
                        except json.JSONDecodeError:
                            logging.warning("Skipping malformed JSON.")
                            continue

                        row = parse_event(event)
                        if row is None:
                            continue

                        try:
                            with connection.cursor() as cursor:
                                cursor.execute(INSERT_SQL, row)
                                did_insert = cursor.rowcount == 1
                            connection.commit()

                            if did_insert:
                                inserted += 1
                                if inserted % 100 == 0:
                                    logging.info(
                                        "New events inserted this run: %s",
                                        inserted,
                                    )
                        except psycopg2.Error:
                            connection.rollback()
                            logging.exception("Database insert failed; reconnecting.")
                            connection.close()
                            connection = None
                            time.sleep(2)
                            break

            except (requests.RequestException, OSError) as exc:
                logging.warning(
                    "Wikimedia stream interrupted: %s. Reconnecting in 5 seconds.",
                    exc,
                )
                time.sleep(5)

                # Reconnect to PostgreSQL and reacquire the single-instance lock.
                if connection is None or connection.closed:
                    try:
                        connection = get_connection()
                        acquire_single_instance_lock(connection)
                        logging.info("PostgreSQL reconnected; ingestor lock acquired.")
                    except Exception:
                        logging.exception("PostgreSQL reconnection failed.")
                        if connection is not None:
                            try:
                                connection.close()
                            except Exception:
                                pass
                        connection = None
                        time.sleep(5)

    except RuntimeError:
        logging.exception("Ingestor could not start.")
    except KeyboardInterrupt:
        logging.info("Stopping ingestor.")
    except psycopg2.Error:
        logging.exception("Ingestor could not connect to PostgreSQL.")
    finally:
        if connection is not None and not connection.closed:
            connection.close()
        logging.info("Ingestor stopped; database connection closed.")


if __name__ == "__main__":
    main()
