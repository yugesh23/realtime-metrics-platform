"""
Integration tests for alert lifecycle transitions: Acknowledge, Resolve, and Re-open.
Runs inside isolated transactions with automatic rollback.
"""

from datetime import datetime, timezone
import pytest
from db import get_connection


@pytest.fixture
def db_conn():
    conn = get_connection("pytest_lifecycle_worker")
    yield conn
    try:
        conn.rollback()
    except Exception:
        pass
    conn.close()


def test_alert_lifecycle_flow(db_conn):
    """Test full alert lifecycle: Open -> Acknowledged -> Resolved."""
    mock_wiki = "__pytest_mock_lifecycle_wiki__"
    test_minute = datetime(2026, 10, 28, 12, 30, 0, tzinfo=timezone.utc)

    with db_conn.cursor() as cur:
        # 1. Create open alert
        cur.execute(
            """
            INSERT INTO alerts (
                metric, wiki, source_minute, value, baseline_mean, baseline_std,
                zscore, severity, status, incident_count, is_historical_duplicate
            )
            VALUES ('edit_activity', %s, %s, 150, 40, 10, 11.0, 'critical', 'open', 1, FALSE)
            RETURNING id, status;
            """,
            (mock_wiki, test_minute),
        )
        alert_id, initial_status = cur.fetchone()
        assert initial_status == "open"

        # 2. Acknowledge alert
        ack_user = "oncall_engineer_alice"
        cur.execute(
            """
            UPDATE alerts
            SET status = 'acknowledged',
                acknowledged_at = NOW(),
                acknowledged_by = %s
            WHERE id = %s
            RETURNING status, acknowledged_by, acknowledged_at;
            """,
            (ack_user, alert_id),
        )
        ack_status, ack_by, ack_at = cur.fetchone()
        assert ack_status == "acknowledged"
        assert ack_by == ack_user
        assert ack_at is not None

        # 3. Resolve alert with notes
        resolution_msg = "Traffic spike identified as legitimate breaking news event. Resolved."
        cur.execute(
            """
            UPDATE alerts
            SET status = 'resolved',
                resolved_at = NOW(),
                resolution_note = %s
            WHERE id = %s
            RETURNING status, resolved_at, resolution_note;
            """,
            (resolution_msg, alert_id),
        )
        res_status, res_at, res_note = cur.fetchone()
        assert res_status == "resolved"
        assert res_at is not None
        assert res_note == resolution_msg

    db_conn.rollback()
