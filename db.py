"""
Database connection, health check, and schema utilities.
Centralized database handling for Real-Time Metrics Monitoring Platform.
"""

import json
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_db_config():
    """Retrieve database configuration from environment variables."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "realtime_metrics"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD"),
        "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
    }


def get_connection(application_name="realtime_metrics_app"):
    """Establish and return a connection to PostgreSQL."""
    config = get_db_config()
    if not config["password"]:
        raise RuntimeError("DB_PASSWORD is not set in environment or .env file.")

    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        connect_timeout=config["connect_timeout"],
        application_name=application_name,
    )


def test_connection():
    """Check connectivity to PostgreSQL and return basic server metadata."""
    conn = get_connection("connection_check")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), version(), NOW();")
            db, ver, now = cur.fetchone()
        return True, {"database": db, "version": ver.split(",")[0], "current_time": now}
    except Exception as exc:
        return False, {"error": str(exc)}
    finally:
        conn.close()


def update_health(conn, component, status, details=None):
    """Update component heartbeat and telemetry in pipeline_health table."""
    if details is None:
        details = {}
    details_json = json.dumps(details, default=str)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_health (component, last_heartbeat, status, details)
            VALUES (%s, NOW(), %s, %s::jsonb)
            ON CONFLICT (component) DO UPDATE SET
                last_heartbeat = NOW(),
                status = EXCLUDED.status,
                details = EXCLUDED.details;
            """,
            (component, status, details_json),
        )
    conn.commit()


def get_health(conn, component=None):
    """Retrieve health records for one or all pipeline components."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if component:
            cur.execute(
                """
                SELECT component, last_heartbeat, status, details
                FROM pipeline_health
                WHERE component = %s;
                """,
                (component,),
            )
            return cur.fetchone()
        else:
            cur.execute(
                """
                SELECT component, last_heartbeat, status, details
                FROM pipeline_health
                ORDER BY component;
                """
            )
            return cur.fetchall()
