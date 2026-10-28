"""
Integration tests for database idempotency, unique constraints, and transaction rollback.
All tests run inside transactions that are rolled back to prevent modifying development data.
"""

from datetime import datetime, timezone
import pytest
from db import get_connection


@pytest.fixture
def db_conn():
    """Provides a database connection with automatic rollback after every test."""
    conn = get_connection("pytest_test_worker")
    yield conn
    # Ensure rollback and close
    try:
        conn.rollback()
    except Exception:
        pass
    conn.close()


def test_unique_alert_constraint_enforcement(db_conn):
    """Verify that inserting duplicate active alerts for the same (metric, wiki, source_minute) is prevented."""
    test_minute = datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)
    mock_wiki = "__pytest_mock_wiki_unique__"

    with db_conn.cursor() as cur:
        # First insert
        cur.execute(
            """
            INSERT INTO alerts (
                metric, wiki, source_minute, value, baseline_mean, baseline_std,
                zscore, severity, status, incident_count, is_historical_duplicate
            )
            VALUES ('edit_activity', %s, %s, 100, 50, 10, 5.0, 'high', 'open', 1, FALSE);
            """,
            (mock_wiki, test_minute),
        )

        # Second insert without ON CONFLICT should violate unique partial index
        with pytest.raises(Exception) as excinfo:
            cur.execute(
                """
                INSERT INTO alerts (
                    metric, wiki, source_minute, value, baseline_mean, baseline_std,
                    zscore, severity, status, incident_count, is_historical_duplicate
                )
                VALUES ('edit_activity', %s, %s, 110, 50, 10, 6.0, 'critical', 'open', 1, FALSE);
                """,
                (mock_wiki, test_minute),
            )
        assert "idx_alerts_unique_identity" in str(excinfo.value) or "unique" in str(excinfo.value).lower()

    # Explicit rollback
    db_conn.rollback()


def test_alert_upsert_idempotency(db_conn):
    """Verify that ON CONFLICT DO UPDATE properly updates the row and increments incident_count without duplicating."""
    test_minute = datetime(2026, 10, 28, 12, 5, 0, tzinfo=timezone.utc)
    mock_wiki = "__pytest_mock_wiki_upsert__"

    upsert_sql = """
        INSERT INTO alerts (
            metric, wiki, source_minute, value, baseline_mean, baseline_std,
            zscore, severity, status, incident_count, is_historical_duplicate
        )
        VALUES ('edit_activity', %s, %s, %s, 50, 10, %s, 'high', 'open', 1, FALSE)
        ON CONFLICT (metric, wiki, source_minute) WHERE is_historical_duplicate = FALSE
        DO UPDATE SET
            value = EXCLUDED.value,
            zscore = EXCLUDED.zscore,
            last_observed_at = NOW(),
            incident_count = alerts.incident_count + 1
        RETURNING id, incident_count, value;
    """

    with db_conn.cursor() as cur:
        # First run: inserts row with incident_count = 1
        cur.execute(upsert_sql, (mock_wiki, test_minute, 100, 5.0))
        row1 = cur.fetchone()
        alert_id_1, incident_count_1, value_1 = row1
        assert incident_count_1 == 1
        assert value_1 == 100

        # Second run for same minute: updates row, incident_count becomes 2, value becomes 125
        cur.execute(upsert_sql, (mock_wiki, test_minute, 125, 7.5))
        row2 = cur.fetchone()
        alert_id_2, incident_count_2, value_2 = row2
        assert alert_id_2 == alert_id_1  # Exact same record!
        assert incident_count_2 == 2
        assert value_2 == 125

        # Check total count for this wiki
        cur.execute("SELECT count(*) FROM alerts WHERE wiki = %s;", (mock_wiki,))
        total_count = cur.fetchone()[0]
        assert total_count == 1  # Exactly ONE alert, no duplication!

    db_conn.rollback()


def test_metric_minute_upsert_idempotency(db_conn):
    """Verify that metric_minute_processed upsert updates metrics cleanly."""
    test_minute = datetime(2026, 10, 28, 12, 10, 0, tzinfo=timezone.utc)
    mock_wiki = "__pytest_mock_wiki_metric__"

    upsert_sql = """
        INSERT INTO metric_minute_processed (wiki, minute, edits, bot_share_pct, avg_abs_edit_size)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (wiki, minute) DO UPDATE SET
            edits = EXCLUDED.edits,
            bot_share_pct = EXCLUDED.bot_share_pct,
            avg_abs_edit_size = EXCLUDED.avg_abs_edit_size
        RETURNING edits;
    """

    with db_conn.cursor() as cur:
        cur.execute(upsert_sql, (mock_wiki, test_minute, 10, 20.0, 50.0))
        assert cur.fetchone()[0] == 10

        cur.execute(upsert_sql, (mock_wiki, test_minute, 25, 40.0, 60.0))
        assert cur.fetchone()[0] == 25

        cur.execute("SELECT count(*) FROM metric_minute_processed WHERE wiki = %s;", (mock_wiki,))
        assert cur.fetchone()[0] == 1

    db_conn.rollback()
