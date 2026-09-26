"""
ride_event_generator.py
-----------------------
Generates realistic fake ride-share events as Python dicts.
No Kafka dependency here — pure data generation logic, fully testable in isolation.

Event schema
------------
{
    "event_id":       str   — UUID4, globally unique per event
    "trip_id":        str   — UUID4, shared across status transitions for the same ride
    "driver_id":      str   — "DRV-<8 hex chars>"
    "rider_id":       str   — "RDR-<8 hex chars>"
    "status":         str   — one of: requested | accepted | started | completed | cancelled
    "city":           str   — city name from CITY_BOUNDING_BOXES
    "pickup": {
        "lat":        float
        "lon":        float
        "address":    str   — human-readable fake address
    }
    "dropoff": {
        "lat":        float
        "lon":        float
        "address":    str
    }
    "fare_usd":       float — base fare before surge, rounded to 2 dp
    "surge_multiplier": float — 1.0–3.5, rounded to 2 dp
    "total_fare_usd": float — fare_usd * surge_multiplier, rounded to 2 dp
    "distance_miles": float — straight-line approx, rounded to 2 dp
    "timestamp":      str   — ISO-8601 UTC, e.g. "2026-09-25T14:30:00.123456Z"
    "event_version":  str   — schema version tag, always "1.0"
}
"""

import math
import random
import uuid
from datetime import datetime, timezone
from typing import Any

from faker import Faker

from config.settings import (
    CITY_BOUNDING_BOXES,
    FARE_MAX_USD,
    FARE_MIN_USD,
    PRODUCER_RANDOM_SEED,
    RIDE_STATUS_CHOICES,
    RIDE_STATUS_WEIGHTS,
    SURGE_MAX,
    SURGE_MIN,
)

# ---------------------------------------------------------------------------
# Initialise Faker and optional seed
# ---------------------------------------------------------------------------

fake = Faker()

if PRODUCER_RANDOM_SEED is not None:
    Faker.seed(PRODUCER_RANDOM_SEED)
    random.seed(PRODUCER_RANDOM_SEED)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _random_coords(city: str) -> tuple[float, float]:
    """Return a (lat, lon) pair uniformly sampled inside a city's bounding box."""
    lat_min, lat_max, lon_min, lon_max = CITY_BOUNDING_BOXES[city]
    lat = random.uniform(lat_min, lat_max)
    lon = random.uniform(lon_min, lon_max)
    return round(lat, 6), round(lon, 6)


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Straight-line distance between two (lat, lon) points using the Haversine formula.
    Returns miles rounded to 2 decimal places.
    """
    R = 3_958.8  # Earth radius in miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return round(2 * R * math.asin(math.sqrt(a)), 2)


def _driver_id() -> str:
    return f"DRV-{uuid.uuid4().hex[:8].upper()}"


def _rider_id() -> str:
    return f"RDR-{uuid.uuid4().hex[:8].upper()}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_ride_event(
    trip_id: str | None = None,
    status: str | None = None,
    city: str | None = None,
) -> dict[str, Any]:
    """
    Generate a single ride-share event dict.

    Parameters
    ----------
    trip_id : str, optional
        Reuse an existing trip ID to simulate status transitions for the same ride.
        If None a new UUID4 is created.
    status : str, optional
        Force a specific status value. If None one is chosen randomly using the
        weights defined in RIDE_STATUS_CHOICES / RIDE_STATUS_WEIGHTS.
    city : str, optional
        Force a specific city. If None a city is chosen at random.

    Returns
    -------
    dict
        A fully populated ride event ready to be serialised and published to Kafka.
    """
    # --- identifiers ---
    event_id = str(uuid.uuid4())
    trip_id = trip_id or str(uuid.uuid4())
    driver_id = _driver_id()
    rider_id = _rider_id()

    # --- location ---
    chosen_city = city or random.choice(list(CITY_BOUNDING_BOXES.keys()))
    pickup_lat, pickup_lon = _random_coords(chosen_city)
    dropoff_lat, dropoff_lon = _random_coords(chosen_city)

    # --- financials ---
    fare = round(random.uniform(FARE_MIN_USD, FARE_MAX_USD), 2)
    surge = round(random.uniform(SURGE_MIN, SURGE_MAX), 2)
    total = round(fare * surge, 2)

    # --- distance ---
    distance = _haversine_miles(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)

    # --- status ---
    chosen_status = status or random.choices(RIDE_STATUS_CHOICES, weights=RIDE_STATUS_WEIGHTS, k=1)[0]

    return {
        "event_id": event_id,
        "trip_id": trip_id,
        "driver_id": driver_id,
        "rider_id": rider_id,
        "status": chosen_status,
        "city": chosen_city,
        "pickup": {
            "lat": pickup_lat,
            "lon": pickup_lon,
            "address": fake.street_address(),
        },
        "dropoff": {
            "lat": dropoff_lat,
            "lon": dropoff_lon,
            "address": fake.street_address(),
        },
        "fare_usd": fare,
        "surge_multiplier": surge,
        "total_fare_usd": total,
        "distance_miles": distance,
        "timestamp": _utc_now_iso(),
        "event_version": "1.0",
    }


def generate_trip_lifecycle(city: str | None = None) -> list[dict[str, Any]]:
    """
    Generate the full ordered sequence of events for a single trip:
    requested → accepted → started → completed

    Useful for end-to-end testing of downstream consumers that track trip state.

    Returns
    -------
    list[dict]
        Four events sharing the same trip_id, each with a different status.
    """
    trip_id = str(uuid.uuid4())
    chosen_city = city or random.choice(list(CITY_BOUNDING_BOXES.keys()))
    statuses = ["requested", "accepted", "started", "completed"]
    return [
        generate_ride_event(trip_id=trip_id, status=s, city=chosen_city)
        for s in statuses
    ]
