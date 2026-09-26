"""
dashboard/app.py
-----------------
Real-Time Ride Analytics Dashboard — built with Streamlit + Plotly.

Reads KPI data directly from PostgreSQL (written by PySpark) and
displays live charts that auto-refresh every 30 seconds.

Run:
  streamlit run dashboard/app.py
"""

import os
import time

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="🚗 Real-Time Ride Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------

PG_HOST     = os.getenv("PG_HOST",     "postgres")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "rideanalytics")
PG_USER     = os.getenv("PG_USER",     "spark")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spark")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD, connect_timeout=5,
    )


def query(sql: str) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame. Returns empty DF on error."""
    try:
        with get_connection() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

st.title("🚗 Real-Time Ride Analytics Pipeline")
st.caption("Data refreshes every 30 seconds · PySpark → PostgreSQL · Redpanda")

# ---------------------------------------------------------------------------
# Row 1 — KPI stat cards
# ---------------------------------------------------------------------------

st.subheader("📊 Key Performance Indicators")

col1, col2, col3, col4, col5 = st.columns(5)

rides_df  = query("SELECT COALESCE(SUM(ride_count), 0) AS total FROM rides_per_city")
fare_df   = query("SELECT COALESCE(ROUND(SUM(total_revenue_usd)::numeric,2),0) AS revenue, COALESCE(ROUND(AVG(avg_fare_usd)::numeric,2),0) AS avg_fare, COALESCE(ROUND(AVG(avg_surge)::numeric,2),0) AS avg_surge FROM fare_metrics")
cancel_df = query("SELECT COALESCE(ROUND(AVG(cancellation_rate)::numeric,4),0) AS avg_cancel FROM cancellation_rate")

total_rides   = int(rides_df["total"].iloc[0])   if not rides_df.empty  else 0
total_revenue = float(fare_df["revenue"].iloc[0]) if not fare_df.empty  else 0.0
avg_fare      = float(fare_df["avg_fare"].iloc[0]) if not fare_df.empty else 0.0
avg_surge     = float(fare_df["avg_surge"].iloc[0]) if not fare_df.empty else 0.0
avg_cancel    = float(cancel_df["avg_cancel"].iloc[0]) if not cancel_df.empty else 0.0

col1.metric("🚖 Total Rides",      f"{total_rides:,}")
col2.metric("💰 Total Revenue",    f"${total_revenue:,.2f}")
col3.metric("🏷️ Avg Fare",         f"${avg_fare:.2f}")
col4.metric("⚡ Avg Surge",        f"{avg_surge:.2f}x",
            delta=f"{'⚠️ High' if avg_surge > 2.0 else '✅ Normal'}",
            delta_color="inverse" if avg_surge > 2.0 else "normal")
col5.metric("❌ Cancellation Rate", f"{avg_cancel*100:.1f}%",
            delta=f"{'⚠️ High' if avg_cancel > 0.2 else '✅ Normal'}",
            delta_color="inverse" if avg_cancel > 0.2 else "normal")

st.divider()

# ---------------------------------------------------------------------------
# Row 2 — Time series charts
# ---------------------------------------------------------------------------

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🏙️ Rides per City — per Minute")
    rides_ts = query("""
        SELECT window_start, city, ride_count
        FROM rides_per_city
        ORDER BY window_start
    """)
    if not rides_ts.empty:
        fig = px.line(
            rides_ts,
            x="window_start", y="ride_count", color="city",
            labels={"window_start": "Time", "ride_count": "Rides", "city": "City"},
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=0, r=0, t=30, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

with col_right:
    st.subheader("💵 Avg Fare by City — per Minute")
    fare_ts = query("""
        SELECT window_start, city, avg_fare_usd
        FROM fare_metrics
        ORDER BY window_start
    """)
    if not fare_ts.empty:
        fig = px.line(
            fare_ts,
            x="window_start", y="avg_fare_usd", color="city",
            labels={"window_start": "Time", "avg_fare_usd": "Avg Fare (USD)", "city": "City"},
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=0, r=0, t=30, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

st.divider()

# ---------------------------------------------------------------------------
# Row 3 — Bar charts + Pie chart
# ---------------------------------------------------------------------------

col_a, col_b, col_c = st.columns(3)

with col_a:
    st.subheader("❌ Cancellation Rate by City")
    cancel_city = query("""
        SELECT city, ROUND(AVG(cancellation_rate)::numeric, 4) AS rate
        FROM cancellation_rate
        GROUP BY city ORDER BY rate DESC
    """)
    if not cancel_city.empty:
        cancel_city["rate_pct"] = (cancel_city["rate"] * 100).round(1)
        fig = px.bar(
            cancel_city, x="city", y="rate_pct",
            labels={"city": "City", "rate_pct": "Cancellation Rate (%)"},
            color="rate_pct",
            color_continuous_scale=["green", "orange", "red"],
            range_color=[0, 30],
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

with col_b:
    st.subheader("⚡ Avg Surge by City")
    surge_city = query("""
        SELECT city, ROUND(AVG(avg_surge)::numeric, 2) AS surge
        FROM fare_metrics
        GROUP BY city ORDER BY surge DESC
    """)
    if not surge_city.empty:
        fig = px.bar(
            surge_city, x="city", y="surge",
            labels={"city": "City", "surge": "Avg Surge Multiplier"},
            color="surge",
            color_continuous_scale=["green", "orange", "red"],
            range_color=[1.0, 3.5],
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

with col_c:
    st.subheader("💰 Revenue Share by City")
    rev_city = query("""
        SELECT city, ROUND(SUM(total_revenue_usd)::numeric, 2) AS revenue
        FROM fare_metrics
        GROUP BY city ORDER BY revenue DESC
    """)
    if not rev_city.empty:
        fig = px.pie(
            rev_city, names="city", values="revenue",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

# ---------------------------------------------------------------------------
# Auto-refresh every 30 seconds
# ---------------------------------------------------------------------------

st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")
time.sleep(30)
st.rerun()
