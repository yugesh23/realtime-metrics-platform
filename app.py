
import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

load_dotenv()

st.set_page_config(
    page_title="Real-Time Metrics Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("Real-Time Metrics Monitoring")
st.caption(
    "Wikimedia event ingestion, activity trends, and anomaly monitoring"
)

st_autorefresh(interval=10_000, key="dashboard_refresh")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realtime_metrics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=5,
    )


@st.cache_data(ttl=5)
def load_data():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    wiki,
                    minute,
                    edits,
                    bot_share_pct,
                    avg_abs_edit_size,
                    processed_at
                FROM metric_minute_processed
                WHERE minute >= NOW() - INTERVAL '24 hours'
                ORDER BY minute DESC
                LIMIT 5000
            """)

            metrics = pd.DataFrame(
                cursor.fetchall(),
                columns=[column[0] for column in cursor.description],
            )

            cursor.execute("""
                SELECT
                    id,
                    metric,
                    wiki,
                    value,
                    zscore,
                    fired_at,
                    status
                FROM alerts
                ORDER BY fired_at DESC
                LIMIT 100
            """)

            alerts = pd.DataFrame(
                cursor.fetchall(),
                columns=[column[0] for column in cursor.description],
            )

            cursor.execute("""
                SELECT
                    COUNT(*) AS total_events,
                    MAX(event_ts) AS latest_event
                FROM events_raw
            """)

            total_events, latest_event = cursor.fetchone()

        return metrics, alerts, total_events, latest_event

    finally:
        connection.close()


try:
    metrics, alerts, total_events, latest_event = load_data()

except Exception as exc:
    st.error("Could not load dashboard data from PostgreSQL.")
    st.code(str(exc))
    st.info(
        "Check your .env settings, PostgreSQL status, table definitions, "
        "and whether processor.py has been started."
    )
    st.stop()


if not metrics.empty:
    metrics["minute"] = pd.to_datetime(
        metrics["minute"], utc=True
    )
    metrics["edits"] = pd.to_numeric(
        metrics["edits"], errors="coerce"
    )
    metrics["bot_share_pct"] = pd.to_numeric(
        metrics["bot_share_pct"], errors="coerce"
    )
    metrics["avg_abs_edit_size"] = pd.to_numeric(
        metrics["avg_abs_edit_size"], errors="coerce"
    )

latest_minute = (
    metrics["minute"].max() if not metrics.empty else None
)

latest_rows = (
    metrics[metrics["minute"] == latest_minute]
    if latest_minute is not None
    else pd.DataFrame()
)

open_alerts = (
    int(
        alerts["status"]
        .astype(str)
        .str.lower()
        .eq("open")
        .sum()
    )
    if not alerts.empty
    else 0
)

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Processed metric rows",
    f"{len(metrics):,}",
)

col2.metric(
    "Latest-minute edits",
    f"{latest_rows['edits'].sum():,.0f}"
    if not latest_rows.empty
    else "—",
)

col3.metric(
    "Wikis monitored",
    f"{metrics['wiki'].nunique():,}"
    if not metrics.empty
    else "0",
)

col4.metric("Open alerts", open_alerts)

st.caption(f"Raw events stored: {total_events:,}")

if latest_event is not None:
    st.caption(f"Last raw event received: {latest_event}")

    event_time = pd.Timestamp(latest_event)
    if event_time.tzinfo is None:
        event_time = event_time.tz_localize("UTC")
    else:
        event_time = event_time.tz_convert("UTC")

    lag_seconds = max(
        (pd.Timestamp.now(tz="UTC") - event_time).total_seconds(),
        0,
    )
    st.caption(f"Approximate ingestion lag: {lag_seconds:.1f} seconds")
else:
    st.warning("No raw events have been stored yet.")

if metrics.empty:
    st.warning(
        "No processed metrics are available yet. "
        "Start processor.py and wait for it to process data."
    )
    st.stop()

st.caption(f"Latest processed minute: {latest_minute}")
st.caption("Metric aggregation window: Last 24 hours")

st.subheader("Edit Activity Over Time")

trend = (
    metrics.groupby("minute", as_index=False)["edits"]
    .sum()
    .sort_values("minute")
)

trend = trend.set_index("minute")

all_minutes = pd.date_range(
    start=trend.index.min().floor("min"),
    end=trend.index.max().floor("min"),
    freq="min",
    tz="UTC",
)

trend = trend.reindex(all_minutes, fill_value=0)
trend.index.name = "minute"
trend = trend.reset_index()

fig = px.line(
    trend,
    x="minute",
    y="edits",
    markers=True,
    title="Total edits per minute",
    labels={
        "minute": "Time (UTC)",
        "edits": "Edits",
    },
)

fig.update_layout(
    hovermode="x unified",
    margin=dict(l=20, r=20, t=60, b=20),
)

st.plotly_chart(fig, width="stretch")

left, right = st.columns(2)

with left:
    st.subheader("Latest Activity by Wiki")

    latest_by_wiki = (
        latest_rows.sort_values("edits", ascending=False)
        .head(15)
    )

    st.dataframe(
        latest_by_wiki,
        width="stretch",
        hide_index=True,
    )

with right:
    st.subheader("Alert History")

    if alerts.empty:
        st.info("No alerts have been recorded yet.")
    else:
        st.dataframe(
            alerts,
            width="stretch",
            hide_index=True,
        )

st.caption("Dashboard refreshes every 10 seconds.")
