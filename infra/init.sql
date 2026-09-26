-- ============================================================
-- init.sql — PostgreSQL schema for the Ride Analytics Pipeline
-- ============================================================

-- Create grafana read-only user
CREATE USER grafana WITH PASSWORD 'grafana';
GRANT CONNECT ON DATABASE rideanalytics TO grafana;
GRANT USAGE ON SCHEMA public TO grafana;

-- Grant SELECT on any tables that already exist
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana;

-- Grant SELECT on ALL FUTURE tables created by the spark user
-- (spark uses mode("append") which creates tables — these grants apply automatically)
ALTER DEFAULT PRIVILEGES FOR ROLE spark IN SCHEMA public GRANT SELECT ON TABLES TO grafana;

-- Pre-create tables so Grafana can connect immediately without waiting for Spark
CREATE TABLE IF NOT EXISTS rides_per_city (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    city             TEXT        NOT NULL,
    ride_count       BIGINT      NOT NULL,
    updated_at       TIMESTAMPTZ,
    PRIMARY KEY (window_start, city)
);

CREATE TABLE IF NOT EXISTS fare_metrics (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    city              TEXT        NOT NULL,
    avg_fare_usd      DOUBLE PRECISION,
    avg_surge         DOUBLE PRECISION,
    avg_distance_miles DOUBLE PRECISION,
    total_revenue_usd DOUBLE PRECISION,
    updated_at        TIMESTAMPTZ,
    PRIMARY KEY (window_start, city)
);

CREATE TABLE IF NOT EXISTS cancellation_rate (
    window_start       TIMESTAMPTZ NOT NULL,
    window_end         TIMESTAMPTZ NOT NULL,
    city               TEXT        NOT NULL,
    total_rides        BIGINT,
    cancelled_rides    BIGINT,
    cancellation_rate  DOUBLE PRECISION,
    updated_at         TIMESTAMPTZ,
    PRIMARY KEY (window_start, city)
);

-- Indexes for fast time-range queries from Grafana
CREATE INDEX IF NOT EXISTS idx_rides_window ON rides_per_city (window_start);
CREATE INDEX IF NOT EXISTS idx_fare_window ON fare_metrics (window_start);
CREATE INDEX IF NOT EXISTS idx_cancel_window ON cancellation_rate (window_start);

-- Grant SELECT on the newly created tables to grafana
GRANT SELECT ON rides_per_city, fare_metrics, cancellation_rate TO grafana;
