"""
Real-Time Metrics Monitoring & Statistical Anomaly Platform
Multi-page operational console for live Wikimedia EventStreams,
minute-level aggregation, statistical anomaly scoring, and alert lifecycle management.
"""

import io
import os
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

from db import get_connection, test_connection, get_health

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Real-Time Metrics Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for polished monitoring console appearance
st.markdown("""
<style>
    /* Metric Card Styling */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.6rem;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.82rem;
    }
    .badge-critical { background-color: #ff4d4f; color: white; }
    .badge-high { background-color: #fa8c16; color: white; }
    .badge-medium { background-color: #faad14; color: black; }
    .badge-low { background-color: #52c41a; color: white; }
    .badge-open { background-color: #f5222d; color: white; }
    .badge-acknowledged { background-color: #1890ff; color: white; }
    .badge-resolved { background-color: #52c41a; color: white; }
    .sub-metric {
        font-size: 0.85rem;
        color: #888;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
# CACHED DATA LOADERS
# ==========================================

@st.cache_data(ttl=5)
def fetch_system_overview():
    """Retrieve high-level system indicators and telemetry with minimal latency."""
    conn = get_connection("dashboard_overview")
    try:
        with conn.cursor() as cur:
            # Fast raw events estimate
            cur.execute("""
                SELECT COALESCE(
                    (SELECT (details->>'inserted')::bigint FROM pipeline_health WHERE component = 'ingestor' AND (details->>'inserted')::bigint > 0),
                    (SELECT reltuples::bigint FROM pg_class WHERE relname = 'events_raw')
                );
            """)
            raw_event_count = cur.fetchone()[0] or 0

            # Latest raw event timestamp
            cur.execute("SELECT MAX(event_ts) FROM events_raw;")
            latest_event_ts = cur.fetchone()[0]

            # Latest processed minute
            cur.execute("SELECT MAX(minute) FROM metric_minute_processed;")
            latest_processed_minute = cur.fetchone()[0]

            # Monitored wikis count
            cur.execute("SELECT COUNT(DISTINCT wiki) FROM metric_minute_processed WHERE minute >= NOW() - INTERVAL '24 hours';")
            wiki_count = cur.fetchone()[0] or 0

            # Alert counts by status
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'open') as open_count,
                    COUNT(*) FILTER (WHERE status = 'open' AND severity IN ('critical', 'high')) as urgent_count,
                    COUNT(*) FILTER (WHERE status = 'acknowledged') as ack_count,
                    COUNT(*) FILTER (WHERE status = 'resolved') as resolved_count
                FROM alerts
                WHERE is_historical_duplicate = FALSE;
            """)
            alert_stats = cur.fetchone()

        health_records = get_health(conn)
        return {
            "raw_events": raw_event_count,
            "latest_event_ts": latest_event_ts,
            "latest_processed_minute": latest_processed_minute,
            "wiki_count": wiki_count,
            "open_alerts": alert_stats[0] if alert_stats else 0,
            "urgent_alerts": alert_stats[1] if alert_stats else 0,
            "ack_alerts": alert_stats[2] if alert_stats else 0,
            "resolved_alerts": alert_stats[3] if alert_stats else 0,
            "health": {h["component"]: h for h in health_records},
        }
    finally:
        conn.close()


@st.cache_data(ttl=5)
def fetch_activity_trend(hours=1):
    """Fetch aggregated edit volume trend for time series charts."""
    conn = get_connection("dashboard_trend")
    try:
        query = """
            SELECT
                minute,
                SUM(edits) as edits,
                ROUND(AVG(bot_share_pct), 2) as avg_bot_share,
                ROUND(AVG(avg_abs_edit_size), 2) as avg_edit_size
            FROM metric_minute_processed
            WHERE minute >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY minute
            ORDER BY minute ASC;
        """
        df = pd.read_sql(query, conn, params=(hours,))
        if not df.empty:
            df["minute"] = pd.to_datetime(df["minute"], utc=True)
        return df
    finally:
        conn.close()


@st.cache_data(ttl=5)
def fetch_top_wikis(hours=1, limit=10):
    """Fetch top wikis by edit volume over the specified lookback window."""
    conn = get_connection("dashboard_top_wikis")
    try:
        query = """
            SELECT
                wiki,
                SUM(edits) as total_edits,
                ROUND(AVG(bot_share_pct), 1) as bot_share_pct,
                ROUND(AVG(avg_abs_edit_size), 1) as avg_size_change
            FROM metric_minute_processed
            WHERE minute >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY wiki
            ORDER BY total_edits DESC
            LIMIT %s;
        """
        return pd.read_sql(query, conn, params=(hours, limit))
    finally:
        conn.close()


@st.cache_data(ttl=5)
def fetch_alerts(status_filter=None, severity_filter=None, limit=200):
    """Retrieve alert records with filtering."""
    conn = get_connection("dashboard_alerts")
    try:
        conditions = ["is_historical_duplicate = FALSE"]
        params = []

        if status_filter and status_filter != "all":
            conditions.append("status = %s")
            params.append(status_filter)

        if severity_filter and severity_filter != "all":
            conditions.append("severity = %s")
            params.append(severity_filter)

        where_clause = " WHERE " + " AND ".join(conditions)
        query = f"""
            SELECT
                id, metric, wiki, source_minute, value as observed_edits,
                baseline_mean, baseline_std, zscore, severity,
                status, incident_count, first_detected_at, last_observed_at,
                acknowledged_at, acknowledged_by, resolved_at, resolution_note
            FROM alerts
            {where_clause}
            ORDER BY source_minute DESC, id DESC
            LIMIT %s;
        """
        params.append(limit)
        df = pd.read_sql(query, conn, params=params)
        if not df.empty:
            df["source_minute"] = pd.to_datetime(df["source_minute"], utc=True)
        return df
    finally:
        conn.close()


def update_alert_lifecycle(alert_id, new_status, operator=None, note=None):
    """Update alert status safely using a transactional query."""
    conn = get_connection("alert_action")
    try:
        with conn.cursor() as cur:
            if new_status == "acknowledged":
                cur.execute("""
                    UPDATE alerts
                    SET status = 'acknowledged',
                        acknowledged_at = NOW(),
                        acknowledged_by = %s
                    WHERE id = %s;
                """, (operator or "Console Operator", alert_id))
            elif new_status == "resolved":
                cur.execute("""
                    UPDATE alerts
                    SET status = 'resolved',
                        resolved_at = NOW(),
                        resolution_note = %s
                    WHERE id = %s;
                """, (note or "Resolved from dashboard console", alert_id))
            elif new_status == "open":
                cur.execute("""
                    UPDATE alerts
                    SET status = 'open',
                        acknowledged_at = NULL,
                        acknowledged_by = NULL,
                        resolved_at = NULL
                    WHERE id = %s;
                """, (alert_id,))
        conn.commit()
        return True, "Alert status updated successfully."
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


# ==========================================
# SIDEBAR NAVIGATION & CONTROLS
# ==========================================

st.sidebar.title("⚡ Real-Time Metrics")
st.sidebar.caption("Wikimedia Stream Anomaly & Monitoring Platform")

page = st.sidebar.radio(
    "Navigation",
    [
        "🏠 Overview",
        "📈 Live Activity",
        "⚠️ Anomaly Monitoring",
        "🔔 Alert Management",
        "🌐 Wiki Explorer",
        "🔍 Data Explorer",
        "🩺 System Health",
    ],
    index=0,
)

st.sidebar.markdown("---")
refresh_rate = st.sidebar.selectbox(
    "🔄 Auto-Refresh Interval",
    options=[0, 5, 10, 30, 60],
    format_func=lambda x: "Off (Manual)" if x == 0 else f"Every {x} seconds",
    index=2,  # 10 seconds default
)

if refresh_rate > 0:
    st_autorefresh(interval=refresh_rate * 1000, key="global_autorefresh")

if st.sidebar.button("🔁 Refresh Data Now"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(
    "**Architecture Note:**\n"
    "• Ingestor: SSE stream → Micro-batch → PostgreSQL\n"
    "• Processor: Minute-level rollups & Z-Score anomaly engine\n"
    "• Deduplication: Enforced by DB-level unique constraint"
)


# ==========================================
# PAGE 1: OVERVIEW
# ==========================================

if page == "🏠 Overview":
    st.title("System Overview & Live Operations")
    st.caption("High-level telemetry, ingestion freshness, and statistical anomaly indicators")

    try:
        overview = fetch_system_overview()
    except Exception as exc:
        st.error(f"Unable to connect to PostgreSQL: {exc}")
        st.stop()

    # Calculate Ingestion Lag
    now_utc = datetime.now(timezone.utc)
    latest_event = overview["latest_event_ts"]
    if latest_event:
        if latest_event.tzinfo is None:
            latest_event = latest_event.replace(tzinfo=timezone.utc)
        lag_seconds = max((now_utc - latest_event).total_seconds(), 0)
    else:
        lag_seconds = None

    # Status Banner
    ingestor_health = overview["health"].get("ingestor", {})
    ingestor_status = ingestor_health.get("status", "unknown")

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Raw Events Recorded", f"{overview['raw_events']:,}")
    col2.metric("Monitored Wikis (24h)", f"{overview['wiki_count']:,}")
    col3.metric("Open Alerts", f"{overview['open_alerts']}", delta=f"{overview['urgent_alerts']} Critical/High", delta_color="inverse")
    
    if lag_seconds is not None:
        lag_display = f"{lag_seconds:.1f}s"
        lag_delta = "Healthy" if lag_seconds < 60 else ("Delayed" if lag_seconds < 300 else "Stale")
        col4.metric("Ingestion Lag", lag_display, delta=lag_delta, delta_color="normal" if lag_seconds < 60 else "inverse")
    else:
        col4.metric("Ingestion Lag", "—")

    col5.metric(
        "Stream Status",
        ingestor_status.upper(),
        delta="Connected" if ingestor_status == "streaming" else "Check Ingestor",
        delta_color="normal" if ingestor_status == "streaming" else "off",
    )

    st.markdown("---")

    # Time series activity
    time_window_col, _ = st.columns([2, 6])
    with time_window_col:
        trend_hours = st.selectbox("Lookback Window", [1, 3, 6, 12, 24], index=0, format_func=lambda x: f"Last {x} Hours")

    trend_df = fetch_activity_trend(hours=trend_hours)

    if not trend_df.empty:
        fig = px.area(
            trend_df,
            x="minute",
            y="edits",
            title=f"Global Edit Activity (Last {trend_hours} Hours)",
            labels={"minute": "UTC Time", "edits": "Total Edits / Minute"},
            color_discrete_sequence=["#1890ff"],
        )
        fig.update_layout(
            hovermode="x unified",
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis_title="Time (UTC)",
            yaxis_title="Edits per Minute",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No processed metric data recorded in this time window.")

    # Two columns: Top Wikis and Recent Alerts
    left_col, right_col = st.columns([1, 1])

    with left_col:
        st.subheader("🏆 Top Wikis by Edit Volume")
        top_wikis_df = fetch_top_wikis(hours=trend_hours, limit=10)
        if not top_wikis_df.empty:
            st.dataframe(
                top_wikis_df.rename(columns={
                    "wiki": "Wiki Project",
                    "total_edits": "Total Edits",
                    "bot_share_pct": "Bot %",
                    "avg_size_change": "Avg Delta (Bytes)",
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.write("No wiki activity in this period.")

    with right_col:
        st.subheader("⚠️ Recent Statistical Anomalies")
        recent_alerts = fetch_alerts(limit=8)
        if not recent_alerts.empty:
            disp_alerts = recent_alerts[[
                "wiki", "source_minute", "observed_edits", "zscore", "severity", "status"
            ]].copy()
            st.dataframe(
                disp_alerts.rename(columns={
                    "wiki": "Wiki",
                    "source_minute": "Event Minute (UTC)",
                    "observed_edits": "Edits",
                    "zscore": "Z-Score",
                    "severity": "Severity",
                    "status": "Status",
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.success("No anomalies detected in the current window.")


# ==========================================
# PAGE 2: LIVE ACTIVITY
# ==========================================

elif page == "📈 Live Activity":
    st.title("Live Wikimedia Activity Deep-Dive")
    st.caption("Time-series event trends, bot participation ratios, and edit payload dynamics")

    filter_col1, filter_col2 = st.columns([2, 4])
    with filter_col1:
        lookback = st.selectbox("Activity Window", [1, 2, 6, 12, 24], index=0, format_func=lambda x: f"Last {x} Hours")

    activity_df = fetch_activity_trend(hours=lookback)

    if activity_df.empty:
        st.warning("No activity records available for this time range.")
        st.stop()

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Total Edits in Window", f"{activity_df['edits'].sum():,}")
    kpi2.metric("Peak Edits / Min", f"{activity_df['edits'].max():,}")
    kpi3.metric("Average Bot Share", f"{activity_df['avg_bot_share'].mean():.1f}%")
    kpi4.metric("Avg Edit Size Delta", f"{activity_df['avg_edit_size'].mean():.1f} bytes")

    # Chart 1: Edits & Bot Share dual axis
    fig_dual = go.Figure()
    fig_dual.add_trace(go.Scatter(
        x=activity_df["minute"],
        y=activity_df["edits"],
        name="Edits / Min",
        mode="lines+markers",
        line=dict(color="#1890ff", width=2),
    ))
    fig_dual.add_trace(go.Scatter(
        x=activity_df["minute"],
        y=activity_df["avg_bot_share"],
        name="Bot Share %",
        mode="lines",
        yaxis="y2",
        line=dict(color="#722ed1", width=2, dash="dot"),
    ))
    fig_dual.update_layout(
        title="Edit Activity vs Bot Automation Share",
        xaxis=dict(title="Time (UTC)"),
        yaxis=dict(title="Edits per Minute"),
        yaxis2=dict(title="Bot Share (%)", overlaying="y", side="right", range=[0, 100]),
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig_dual, use_container_width=True)

    # Chart 2: Edit size variation
    fig_size = px.line(
        activity_df,
        x="minute",
        y="avg_edit_size",
        title="Average Edit Byte-Size Variation (Bytes Added or Removed)",
        labels={"minute": "Time (UTC)", "avg_edit_size": "Avg Absolute Delta (Bytes)"},
        color_discrete_sequence=["#13c2c2"],
    )
    fig_size.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_size, use_container_width=True)


# ==========================================
# PAGE 3: ANOMALY MONITORING
# ==========================================

elif page == "⚠️ Anomaly Monitoring":
    st.title("Statistical Anomaly Detection")
    st.caption("Rolling baseline Z-Score detection engine (|Z| ≥ 3.0)")

    st.info(
        "💡 **Operational Note:** Detected anomalies represent statistically significant deviations "
        "relative to a wiki's 60-minute historical moving baseline. They indicate unusual spikes in edit volume "
        "and do **not** indicate malicious activity or security compromise."
    )

    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        sev_choice = st.selectbox("Filter Severity", ["all", "critical", "high", "medium"])
    with fcol2:
        status_choice = st.selectbox("Filter Status", ["all", "open", "acknowledged", "resolved"])
    with fcol3:
        max_rows = st.selectbox("Max Alerts to Display", [50, 100, 250, 500], index=1)

    alerts_df = fetch_alerts(status_filter=status_choice, severity_filter=sev_choice, limit=max_rows)

    if alerts_df.empty:
        st.success("No anomalies match the selected filters.")
    else:
        # Scatter Plot: Observed vs Baseline Mean
        fig_scatter = px.scatter(
            alerts_df,
            x="baseline_mean",
            y="observed_edits",
            color="severity",
            size="observed_edits",
            hover_data=["wiki", "source_minute", "zscore", "status"],
            title="Observed Edit Rate vs Historical Baseline Mean",
            labels={
                "baseline_mean": "Baseline Moving Mean (Edits/Min)",
                "observed_edits": "Observed Spike (Edits/Min)",
                "severity": "Severity",
            },
            color_discrete_map={
                "critical": "#ff4d4f",
                "high": "#fa8c16",
                "medium": "#faad14",
                "low": "#52c41a",
            },
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.subheader("Detected Anomaly Records")
        st.dataframe(
            alerts_df[[
                "id", "wiki", "source_minute", "observed_edits", "baseline_mean",
                "baseline_std", "zscore", "severity", "incident_count", "status"
            ]].rename(columns={
                "id": "Alert ID",
                "wiki": "Wiki",
                "source_minute": "Source Minute (UTC)",
                "observed_edits": "Observed",
                "baseline_mean": "Baseline Mean",
                "baseline_std": "StdDev",
                "zscore": "Z-Score",
                "severity": "Severity",
                "incident_count": "Cycles",
                "status": "Status",
            }),
            use_container_width=True,
            hide_index=True,
        )


# ==========================================
# PAGE 4: ALERT MANAGEMENT
# ==========================================

elif page == "🔔 Alert Management":
    st.title("Alert Lifecycle Operations Console")
    st.caption("Acknowledge, triage, and resolve detected operational incidents with full transactional audit logging")

    tab_open, tab_ack, tab_res, tab_all = st.tabs([
        "🚨 Open Alerts", "👀 Acknowledged", "✅ Resolved", "📋 All Alerts"
    ])

    def render_alert_table(status_filter, tab_key):
        df = fetch_alerts(status_filter=status_filter, limit=100)
        if df.empty:
            st.info(f"No {status_filter or 'active'} alerts recorded.")
            return

        st.dataframe(
            df[[
                "id", "wiki", "source_minute", "observed_edits", "zscore", "severity",
                "incident_count", "status", "acknowledged_by", "resolution_note"
            ]],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### Triage Action Panel")
        action_col1, action_col2 = st.columns([1, 2])

        with action_col1:
            selected_id = st.selectbox(
                "Select Alert ID to Action",
                options=df["id"].tolist(),
                key=f"select_alert_{tab_key}",
            )

        selected_row = df[df["id"] == selected_id].iloc[0]

        with action_col2:
            st.markdown(
                f"**Alert #{selected_id}** on **{selected_row['wiki']}** | "
                f"Observed: `{selected_row['observed_edits']} edits` | "
                f"Z-Score: `{selected_row['zscore']}` | Severity: `{selected_row['severity']}` | "
                f"Occurrences: `{selected_row['incident_count']}`"
            )

        btn_col1, btn_col2, btn_col3 = st.columns(3)

        if selected_row["status"] == "open":
            with btn_col1:
                operator_name = st.text_input("Operator Name", value="OnCall-Engineer", key=f"op_{tab_key}")
                if st.button("👁️ Acknowledge Alert", key=f"ack_btn_{tab_key}"):
                    success, msg = update_alert_lifecycle(selected_id, "acknowledged", operator=operator_name)
                    if success:
                        st.success(f"Alert #{selected_id} acknowledged.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"Action failed: {msg}")

        if selected_row["status"] in ["open", "acknowledged"]:
            with btn_col2:
                res_note = st.text_input("Resolution Note", value="Normal traffic burst verified", key=f"note_{tab_key}")
                if st.button("✅ Mark Resolved", key=f"res_btn_{tab_key}"):
                    success, msg = update_alert_lifecycle(selected_id, "resolved", note=res_note)
                    if success:
                        st.success(f"Alert #{selected_id} resolved.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"Action failed: {msg}")

        if selected_row["status"] in ["acknowledged", "resolved"]:
            with btn_col3:
                if st.button("🔄 Reopen Alert", key=f"reopen_btn_{tab_key}"):
                    success, msg = update_alert_lifecycle(selected_id, "open")
                    if success:
                        st.info(f"Alert #{selected_id} reopened.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"Action failed: {msg}")

    with tab_open:
        render_alert_table("open", "open_tab")

    with tab_ack:
        render_alert_table("acknowledged", "ack_tab")

    with tab_res:
        render_alert_table("resolved", "res_tab")

    with tab_all:
        render_alert_table("all", "all_tab")


# ==========================================
# PAGE 5: WIKI EXPLORER
# ==========================================

elif page == "🌐 Wiki Explorer":
    st.title("Wiki Project Explorer")
    st.caption("Deep dive into specific Wikipedia, Wikidata, Wiktionary, or Wikimedia language editions")

    # Fetch available wikis
    conn = get_connection("wiki_explorer_list")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wiki, SUM(edits) as total_edits
                FROM metric_minute_processed
                GROUP BY wiki
                ORDER BY total_edits DESC
                LIMIT 50;
            """)
            wiki_options = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not wiki_options:
        st.warning("No wiki metrics available yet.")
        st.stop()

    selected_wiki = st.selectbox("Select Wiki Project", wiki_options, index=0)

    # Fetch Wiki specifics
    conn = get_connection("wiki_details")
    try:
        wiki_metrics = pd.read_sql("""
            SELECT minute, edits, bot_share_pct, avg_abs_edit_size
            FROM metric_minute_processed
            WHERE wiki = %s
            ORDER BY minute ASC;
        """, conn, params=(selected_wiki,))

        wiki_alerts = pd.read_sql("""
            SELECT id, source_minute, value as observed_edits, baseline_mean, zscore, severity, status
            FROM alerts
            WHERE wiki = %s AND is_historical_duplicate = FALSE
            ORDER BY source_minute DESC;
        """, conn, params=(selected_wiki,))
    finally:
        conn.close()

    if not wiki_metrics.empty:
        wiki_metrics["minute"] = pd.to_datetime(wiki_metrics["minute"], utc=True)

        w1, w2, w3, w4 = st.columns(4)
        w1.metric("Total Recorded Edits", f"{wiki_metrics['edits'].sum():,}")
        w2.metric("Peak Velocity", f"{wiki_metrics['edits'].max():,} edits/min")
        w3.metric("Bot Proportion", f"{wiki_metrics['bot_share_pct'].mean():.1f}%")
        w4.metric("Anomalies Recorded", f"{len(wiki_alerts)}")

        # Wiki timeline
        fig_wiki = px.line(
            wiki_metrics,
            x="minute",
            y="edits",
            title=f"Edit Activity History: {selected_wiki}",
            labels={"minute": "UTC Time", "edits": "Edits / Minute"},
            markers=True,
            color_discrete_sequence=["#52c41a"],
        )
        st.plotly_chart(fig_wiki, use_container_width=True)

        st.subheader(f"Historical Anomalies for {selected_wiki}")
        if not wiki_alerts.empty:
            st.dataframe(wiki_alerts, use_container_width=True, hide_index=True)
        else:
            st.info(f"No statistical anomalies flagged for {selected_wiki}.")


# ==========================================
# PAGE 6: DATA EXPLORER & CSV EXPORT
# ==========================================

elif page == "🔍 Data Explorer":
    st.title("Raw & Analytical Data Explorer")
    st.caption("Safely inspect underlying database tables with pagination and export capabilities")

    table_choice = st.radio(
        "Select Table to Inspect",
        ["metric_minute_processed", "alerts", "events_raw", "alerts_historical_backup"],
        horizontal=True,
    )

    row_limit = st.selectbox("Row Display Limit", [50, 100, 250, 500], index=1)

    conn = get_connection("data_explorer")
    try:
        if table_choice == "events_raw":
            query = """
                SELECT event_id, wiki, event_type, is_bot, user_name, title, event_ts, len_old, len_new
                FROM events_raw
                ORDER BY event_ts DESC
                LIMIT %s;
            """
        elif table_choice == "metric_minute_processed":
            query = """
                SELECT wiki, minute, edits, bot_share_pct, avg_abs_edit_size, processed_at
                FROM metric_minute_processed
                ORDER BY minute DESC
                LIMIT %s;
            """
        elif table_choice == "alerts":
            query = """
                SELECT id, metric, wiki, source_minute, value, zscore, severity, status, incident_count, last_observed_at
                FROM alerts
                ORDER BY id DESC
                LIMIT %s;
            """
        else:
            query = """
                SELECT id, metric, wiki, value, zscore, fired_at, status
                FROM alerts_historical_backup
                ORDER BY id DESC
                LIMIT %s;
            """

        df_table = pd.read_sql(query, conn, params=(row_limit,))
    finally:
        conn.close()

    st.markdown(f"**Showing {len(df_table)} records from `{table_choice}`**")
    st.dataframe(df_table, use_container_width=True, hide_index=True)

    # Export to CSV
    csv_buffer = io.StringIO()
    df_table.to_csv(csv_buffer, index=False)
    st.download_button(
        label=f"📥 Export {table_choice} to CSV",
        data=csv_buffer.getvalue(),
        file_name=f"{table_choice}_export_{int(time.time())}.csv",
        mime="text/csv",
    )


# ==========================================
# PAGE 7: SYSTEM HEALTH
# ==========================================

elif page == "🩺 System Health":
    st.title("System Health & Diagnostic Telemetry")
    st.caption("Real-time pipeline heartbeats, lag estimation, and database operational metrics")

    conn = get_connection("health_monitor")
    try:
        health_data = get_health(conn)
        db_ok, db_meta = test_connection()

        # Database latency check
        t0 = time.time()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        db_latency_ms = (time.time() - t0) * 1000

        # Ingestion lag calculation
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(event_ts) FROM events_raw;")
            latest_event = cur.fetchone()[0]

            cur.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = 'events_raw';")
            approx_events = cur.fetchone()[0]

            cur.execute("SELECT count(*) FROM metric_minute_processed;")
            metric_count = cur.fetchone()[0]

            cur.execute("SELECT count(*) FROM alerts;")
            total_alerts_count = cur.fetchone()[0]
    finally:
        conn.close()

    now_utc = datetime.now(timezone.utc)
    if latest_event:
        if latest_event.tzinfo is None:
            latest_event = latest_event.replace(tzinfo=timezone.utc)
        lag_seconds = max((now_utc - latest_event).total_seconds(), 0)
    else:
        lag_seconds = None

    # Pipeline Component Cards
    h_col1, h_col2, h_col3 = st.columns(3)

    # 1. Ingestor Card
    ingestor_info = next((h for h in health_data if h["component"] == "ingestor"), None)
    with h_col1:
        st.subheader("📥 Ingestion Worker")
        if ingestor_info:
            status = ingestor_info["status"]
            st.metric("Status", status.upper())
            details = ingestor_info.get("details") or {}
            st.write(f"**Last Heartbeat:** `{ingestor_info['last_heartbeat']}`")
            st.write(f"**Uptime:** `{details.get('uptime_seconds', 0)}s`")
            st.write(f"**Received Events:** `{details.get('received', 0):,}`")
            st.write(f"**Inserted Events:** `{details.get('inserted', 0):,}`")
            st.write(f"**Skipped Events:** `{details.get('skipped', 0):,}`")
            st.write(f"**Malformed Events:** `{details.get('malformed', 0):,}`")
        else:
            st.warning("No ingestor heartbeat recorded. Start `ingestor.py`.")

    # 2. Processor Card
    processor_info = next((h for h in health_data if h["component"] == "processor"), None)
    with h_col2:
        st.subheader("⚙️ Processor & Anomaly Engine")
        if processor_info:
            p_status = processor_info["status"]
            st.metric("Status", p_status.upper())
            p_details = processor_info.get("details") or {}
            st.write(f"**Last Heartbeat:** `{processor_info['last_heartbeat']}`")
            st.write(f"**Latest Processed Min:** `{p_details.get('last_processed_minute', 'N/A')}`")
            st.write(f"**Cycle Duration:** `{p_details.get('cycle_duration_seconds', 0)}s`")
            st.write(f"**Z-Score Threshold:** `{p_details.get('zscore_threshold', 3.0)}`")
            st.write(f"**Min Sample Count:** `{p_details.get('min_sample_count', 10)}`")
        else:
            st.warning("No processor heartbeat recorded. Start `processor.py`.")

    # 3. PostgreSQL Database Card
    with h_col3:
        st.subheader("🐘 PostgreSQL Database")
        if db_ok:
            st.metric("Status", "CONNECTED")
            st.write(f"**Database:** `{db_meta.get('database')}`")
            st.write(f"**Version:** `{db_meta.get('version')}`")
            st.write(f"**Query Latency:** `{db_latency_ms:.2f} ms`")
            st.write(f"**Estimated Raw Events:** `{approx_events:,}`")
            st.write(f"**Metric Minutes Stored:** `{metric_count:,}`")
            st.write(f"**Total Alert Records:** `{total_alerts_count:,}`")
        else:
            st.error(f"Database Disconnected: {db_meta.get('error')}")

    st.markdown("---")
    st.subheader("Stream Latency & Freshness")
    if lag_seconds is not None:
        if lag_seconds < 60:
            st.success(f"🟢 **Healthy Stream Freshness:** Event stream lag is {lag_seconds:.1f} seconds.")
        elif lag_seconds < 300:
            st.warning(f"🟡 **Delayed Ingestion:** Event stream lag is {lag_seconds:.1f} seconds.")
        else:
            st.error(f"🔴 **Stale Ingestion Stream:** No new events received in {lag_seconds:.1f} seconds. Verify `ingestor.py`.")
    else:
        st.info("No raw events have been ingested yet.")
