"""
dashboard/app.py
-----------------
Real-Time Ride Analytics Dashboard — Streamlit + Plotly
Reads KPI data from PostgreSQL (written by PySpark) with live filters.
Auto-refreshes every 30 seconds.
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
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------

PG_HOST     = os.getenv("PG_HOST",     "postgres")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "rideanalytics")
PG_USER     = os.getenv("PG_USER",     "spark")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spark")


def query(sql: str, params=None) -> pd.DataFrame:
    try:
        with psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD, connect_timeout=5,
        ) as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar — Filters
# ---------------------------------------------------------------------------

st.sidebar.title("🔎 Filters")

# City filter
all_cities_df = query("SELECT DISTINCT city FROM rides_per_city ORDER BY city")
all_cities = all_cities_df["city"].tolist() if not all_cities_df.empty else []
selected_cities = st.sidebar.multiselect(
    "🏙️ City",
    options=all_cities,
    default=all_cities,
    help="Select one or more cities to display",
)

# Time window filter
time_range = st.sidebar.selectbox(
    "⏱️ Time Range",
    options=["Last 15 minutes", "Last 30 minutes", "Last 1 hour", "Last 2 hours", "All time"],
    index=2,
)

time_map = {
    "Last 15 minutes": "15 minutes",
    "Last 30 minutes": "30 minutes",
    "Last 1 hour":     "1 hour",
    "Last 2 hours":    "2 hours",
    "All time":        None,
}
time_filter = time_map[time_range]

def time_where(col: str = "window_start") -> str:
    if time_filter:
        return f"WHERE {col} >= NOW() - INTERVAL '{time_filter}'"
    return ""

def time_and(col: str = "window_start") -> str:
    if time_filter:
        return f"AND {col} >= NOW() - INTERVAL '{time_filter}'"
    return ""

# City filter SQL helper
def city_filter(col: str = "city") -> str:
    if not selected_cities:
        return "1=0"  # no city selected → show nothing
    placeholders = ",".join([f"'{c}'" for c in selected_cities])
    return f"{col} IN ({placeholders})"

st.sidebar.divider()
st.sidebar.caption("Dashboard auto-refreshes every 30s")
st.sidebar.caption(f"Connected to: `{PG_HOST}:{PG_PORT}/{PG_DATABASE}`")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🚗 Real-Time Ride Analytics Pipeline")
st.caption(f"Showing: **{time_range}** · Cities: **{', '.join(selected_cities) if selected_cities else 'None'}**")

if not selected_cities:
    st.warning("Select at least one city from the sidebar.")
    st.stop()

# ---------------------------------------------------------------------------
# Row 1 — KPI stat cards
# ---------------------------------------------------------------------------

st.subheader("📊 Key Performance Indicators")

col1, col2, col3, col4, col5 = st.columns(5)

rides_kpi = query(f"""
    SELECT COALESCE(SUM(ride_count), 0) AS total
    FROM rides_per_city
    {time_where()}
    {'AND' if time_filter else 'WHERE'} {city_filter()}
""")

fare_kpi = query(f"""
    SELECT
        COALESCE(ROUND(SUM(total_revenue_usd)::numeric,2), 0) AS revenue,
        COALESCE(ROUND(AVG(avg_fare_usd)::numeric,2), 0)      AS avg_fare,
        COALESCE(ROUND(AVG(avg_surge)::numeric,2), 0)          AS avg_surge
    FROM fare_metrics
    {time_where()}
    {'AND' if time_filter else 'WHERE'} {city_filter()}
""")

cancel_kpi = query(f"""
    SELECT COALESCE(ROUND(AVG(cancellation_rate)::numeric,4), 0) AS avg_cancel
    FROM cancellation_rate
    {time_where()}
    {'AND' if time_filter else 'WHERE'} {city_filter()}
""")

total_rides   = int(rides_kpi["total"].iloc[0])    if not rides_kpi.empty  else 0
total_revenue = float(fare_kpi["revenue"].iloc[0]) if not fare_kpi.empty   else 0.0
avg_fare      = float(fare_kpi["avg_fare"].iloc[0]) if not fare_kpi.empty  else 0.0
avg_surge     = float(fare_kpi["avg_surge"].iloc[0]) if not fare_kpi.empty else 0.0
avg_cancel    = float(cancel_kpi["avg_cancel"].iloc[0]) if not cancel_kpi.empty else 0.0

col1.metric("🚖 Total Rides",       f"{total_rides:,}")
col2.metric("💰 Total Revenue",     f"${total_revenue:,.2f}")
col3.metric("🏷️ Avg Fare",          f"${avg_fare:.2f}")
col4.metric("⚡ Avg Surge",         f"{avg_surge:.2f}x",
            delta="⚠️ High" if avg_surge > 2.0 else "✅ Normal",
            delta_color="inverse" if avg_surge > 2.0 else "normal")
col5.metric("❌ Cancellation Rate", f"{avg_cancel*100:.1f}%",
            delta="⚠️ High" if avg_cancel > 0.2 else "✅ Normal",
            delta_color="inverse" if avg_cancel > 0.2 else "normal")

st.divider()

# ---------------------------------------------------------------------------
# Row 2 — Time series
# ---------------------------------------------------------------------------

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🏙️ Rides per City — per Minute")
    rides_ts = query(f"""
        SELECT window_start, city, ride_count
        FROM rides_per_city
        {time_where()} {'AND' if time_filter else 'WHERE'} {city_filter()}
        ORDER BY window_start
    """)
    if not rides_ts.empty:
        fig = px.line(rides_ts, x="window_start", y="ride_count", color="city",
                      labels={"window_start": "Time", "ride_count": "Rides", "city": "City"},
                      color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                          legend=dict(orientation="h", yanchor="bottom", y=1.02),
                          margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col_right:
    st.subheader("💵 Avg Fare by City — per Minute")
    fare_ts = query(f"""
        SELECT window_start, city, avg_fare_usd
        FROM fare_metrics
        {time_where()} {'AND' if time_filter else 'WHERE'} {city_filter()}
        ORDER BY window_start
    """)
    if not fare_ts.empty:
        fig = px.line(fare_ts, x="window_start", y="avg_fare_usd", color="city",
                      labels={"window_start": "Time", "avg_fare_usd": "Avg Fare ($)", "city": "City"},
                      color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                          legend=dict(orientation="h", yanchor="bottom", y=1.02),
                          margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

st.divider()

# ---------------------------------------------------------------------------
# Row 3 — Bar charts + Pie chart
# ---------------------------------------------------------------------------

col_a, col_b, col_c = st.columns(3)

with col_a:
    st.subheader("❌ Cancellation Rate by City")
    cancel_city = query(f"""
        SELECT city, ROUND(AVG(cancellation_rate)::numeric, 4) AS rate
        FROM cancellation_rate
        {time_where()} {'AND' if time_filter else 'WHERE'} {city_filter()}
        GROUP BY city ORDER BY rate DESC
    """)
    if not cancel_city.empty:
        cancel_city["rate_pct"] = (cancel_city["rate"] * 100).round(1)
        fig = px.bar(cancel_city, x="city", y="rate_pct",
                     labels={"city": "City", "rate_pct": "Cancellation (%)"},
                     color="rate_pct",
                     color_continuous_scale=["green", "orange", "red"],
                     range_color=[0, 30])
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                          coloraxis_showscale=False, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col_b:
    st.subheader("⚡ Avg Surge by City")
    surge_city = query(f"""
        SELECT city, ROUND(AVG(avg_surge)::numeric, 2) AS surge
        FROM fare_metrics
        {time_where()} {'AND' if time_filter else 'WHERE'} {city_filter()}
        GROUP BY city ORDER BY surge DESC
    """)
    if not surge_city.empty:
        fig = px.bar(surge_city, x="city", y="surge",
                     labels={"city": "City", "surge": "Avg Surge"},
                     color="surge",
                     color_continuous_scale=["green", "orange", "red"],
                     range_color=[1.0, 3.5])
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                          coloraxis_showscale=False, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col_c:
    st.subheader("💰 Revenue Share by City")
    rev_city = query(f"""
        SELECT city, ROUND(SUM(total_revenue_usd)::numeric, 2) AS revenue
        FROM fare_metrics
        {time_where()} {'AND' if time_filter else 'WHERE'} {city_filter()}
        GROUP BY city ORDER BY revenue DESC
    """)
    if not rev_city.empty:
        fig = px.pie(rev_city, names="city", values="revenue", hole=0.4,
                     color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# ---------------------------------------------------------------------------
# Row 4 — Raw data table (optional, collapsible)
# ---------------------------------------------------------------------------

with st.expander("📋 Raw Data — Rides per City"):
    raw = query(f"""
        SELECT window_start, window_end, city, ride_count, updated_at
        FROM rides_per_city
        {time_where()} {'AND' if time_filter else 'WHERE'} {city_filter()}
        ORDER BY window_start DESC
        LIMIT 100
    """)
    if not raw.empty:
        st.dataframe(raw, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# ---------------------------------------------------------------------------
# Footer + auto-refresh
# ---------------------------------------------------------------------------

st.caption(f"⏱️ Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
time.sleep(30)
st.rerun()
