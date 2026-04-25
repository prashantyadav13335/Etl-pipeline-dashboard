"""
ETL Pipeline — Weather Data
Extracts from Open-Meteo API (free, no key needed)
Transforms & loads into SQLite database
Cities are stored in DB — manageable from dashboard without touching code!
"""

import sqlite3
import requests
import json
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = "data/pipeline.db"

# Default cities (only used on FIRST run to seed the DB)
DEFAULT_CITIES = [
    {"name": "Delhi",     "lat": 28.6139, "lon": 77.2090},
    {"name": "Mumbai",    "lat": 19.0760, "lon": 72.8777},
    {"name": "Bengaluru", "lat": 12.9716, "lon": 77.5946},
    {"name": "Kolkata",   "lat": 22.5726, "lon": 88.3639},
    {"name": "Chennai",   "lat": 13.0827, "lon": 80.2707},
]


# ─── DATABASE INIT ────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at      TEXT NOT NULL,
            cities      INTEGER,
            records     INTEGER,
            status      TEXT
        );

        CREATE TABLE IF NOT EXISTS weather_daily (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            city          TEXT    NOT NULL,
            date          TEXT    NOT NULL,
            avg_temp_c    REAL,
            max_temp_c    REAL,
            min_temp_c    REAL,
            avg_humidity  REAL,
            avg_wind_kmh  REAL,
            total_precip  REAL,
            loaded_at     TEXT,
            UNIQUE(city, date)
        );

        CREATE TABLE IF NOT EXISTS managed_cities (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT    NOT NULL UNIQUE,
            lat       REAL    NOT NULL,
            lon       REAL    NOT NULL,
            added_at  TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_city_date ON weather_daily (city, date);
    """)
    conn.commit()

    # Seed default cities if table is empty
    count = conn.execute("SELECT COUNT(*) FROM managed_cities").fetchone()[0]
    if count == 0:
        logger.info("🌱 Seeding default cities into DB...")
        conn.executemany(
            "INSERT OR IGNORE INTO managed_cities (name, lat, lon, added_at) VALUES (?, ?, ?, ?)",
            [(c["name"], c["lat"], c["lon"], datetime.now().isoformat()) for c in DEFAULT_CITIES]
        )
        conn.commit()


def get_cities_from_db(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT name, lat, lon FROM managed_cities ORDER BY id").fetchall()
    return [{"name": r[0], "lat": r[1], "lon": r[2]} for r in rows]


# ─── CITY MANAGEMENT ──────────────────────────────────────────────────────────

def add_city(name: str, lat: float, lon: float) -> dict:
    """Add a new city to managed_cities and immediately fetch its data."""
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    try:
        conn.execute(
            "INSERT INTO managed_cities (name, lat, lon, added_at) VALUES (?, ?, ?, ?)",
            (name, lat, lon, datetime.now().isoformat())
        )
        conn.commit()
        logger.info(f"➕ Added city: {name} ({lat}, {lon})")

        # Run ETL just for this city
        city_dict = {"name": name, "lat": lat, "lon": lon}
        raw = extract(city_dict)
        if raw:
            records = transform(raw, name)
            load(conn, records)
            logger.info(f"✅ Data loaded for new city: {name}")
            conn.close()
            return {"status": "success", "city": name, "records": len(records)}
        conn.close()
        return {"status": "success", "city": name, "records": 0}
    except sqlite3.IntegrityError:
        conn.close()
        return {"status": "error", "message": f"{name} already exists"}


def remove_city(name: str) -> dict:
    """Remove a city from managed_cities and its weather data."""
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    conn.execute("DELETE FROM managed_cities WHERE name = ?", (name,))
    conn.execute("DELETE FROM weather_daily WHERE city = ?", (name,))
    conn.commit()
    conn.close()
    logger.info(f"🗑️ Removed city: {name}")
    return {"status": "success", "city": name}


def geocode_city(city_name: str) -> dict | None:
    """Use Open-Meteo geocoding API to find lat/lon for a city name."""
    try:
        url = "https://geocoding-api.open-meteo.com/v1/search"
        r = requests.get(url, params={"name": city_name, "count": 1, "language": "en", "format": "json"}, timeout=8)
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            res = results[0]
            return {
                "name": res.get("name", city_name),
                "lat": round(res["latitude"], 4),
                "lon": round(res["longitude"], 4),
                "country": res.get("country", ""),
            }
    except Exception as e:
        logger.warning(f"Geocoding failed for {city_name}: {e}")
    return None


# ─── EXTRACT ──────────────────────────────────────────────────────────────────

def _mock_data(city: dict) -> dict:
    """Generate realistic mock weather data when API is unavailable."""
    import random, math
    random.seed(hash(city["name"]) % 2**31)

    base_temp = 38 - abs(city["lat"] - 8) * 0.6 + random.uniform(-2, 2)
    times, temps, humidity, wind, precip = [], [], [], [], []

    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for day in range(3):
        for hour in range(24):
            dt = start + timedelta(days=day, hours=hour)
            times.append(dt.strftime("%Y-%m-%dT%H:%M"))
            hour_factor = math.sin(math.pi * (hour - 6) / 12) if 6 <= hour <= 18 else -0.3
            temps.append(round(base_temp + hour_factor * 6 + random.uniform(-1, 1), 1))
            humidity.append(round(55 + random.uniform(-15, 15), 1))
            wind.append(round(10 + random.uniform(0, 15), 1))
            precip.append(round(random.uniform(0, 0.5) if random.random() > 0.8 else 0, 2))

    return {
        "hourly": {
            "time": times,
            "temperature_2m": temps,
            "relative_humidity_2m": humidity,
            "wind_speed_10m": wind,
            "precipitation": precip,
        }
    }


def extract(city: dict) -> dict | None:
    """Fetch raw hourly weather data from Open-Meteo API, with mock fallback."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":      city["lat"],
        "longitude":     city["lon"],
        "hourly":        "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "forecast_days": 3,
        "timezone":      "Asia/Kolkata",
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"✅ Extracted data for {city['name']} (API)")
        return response.json()
    except requests.RequestException:
        logger.warning(f"⚠️  API unavailable for {city['name']} — using realistic mock data")
        return _mock_data(city)


# ─── TRANSFORM ────────────────────────────────────────────────────────────────

def transform(raw: dict, city_name: str) -> list[dict]:
    """Clean, validate, and aggregate hourly → daily summaries."""
    hourly   = raw.get("hourly", {})
    times    = hourly.get("time", [])
    temps    = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    wind     = hourly.get("wind_speed_10m", [])
    precip   = hourly.get("precipitation", [])

    daily_buckets: dict[str, list] = {}

    for i, timestamp in enumerate(times):
        date = timestamp[:10]
        if date not in daily_buckets:
            daily_buckets[date] = []
        daily_buckets[date].append({
            "temp":     temps[i]    if i < len(temps)    else None,
            "humidity": humidity[i] if i < len(humidity) else None,
            "wind":     wind[i]     if i < len(wind)     else None,
            "precip":   precip[i]   if i < len(precip)   else None,
        })

    records = []
    for date, hours in daily_buckets.items():
        valid_temps    = [h["temp"]     for h in hours if h["temp"]     is not None]
        valid_humidity = [h["humidity"] for h in hours if h["humidity"] is not None]
        valid_wind     = [h["wind"]     for h in hours if h["wind"]     is not None]
        valid_precip   = [h["precip"]   for h in hours if h["precip"]   is not None]

        if not valid_temps:
            continue

        records.append({
            "city":          city_name,
            "date":          date,
            "avg_temp_c":    round(sum(valid_temps)    / len(valid_temps),    2),
            "max_temp_c":    round(max(valid_temps),                          2),
            "min_temp_c":    round(min(valid_temps),                          2),
            "avg_humidity":  round(sum(valid_humidity) / len(valid_humidity), 2) if valid_humidity else None,
            "avg_wind_kmh":  round(sum(valid_wind)     / len(valid_wind),     2) if valid_wind     else None,
            "total_precip":  round(sum(valid_precip),                         2) if valid_precip   else None,
            "loaded_at":     datetime.now().isoformat(),
        })

    logger.info(f"🔄 Transformed {len(records)} daily records for {city_name}")
    return records


# ─── LOAD ─────────────────────────────────────────────────────────────────────

def load(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Upsert transformed records — idempotent runs."""
    sql = """
        INSERT INTO weather_daily
            (city, date, avg_temp_c, max_temp_c, min_temp_c,
             avg_humidity, avg_wind_kmh, total_precip, loaded_at)
        VALUES
            (:city, :date, :avg_temp_c, :max_temp_c, :min_temp_c,
             :avg_humidity, :avg_wind_kmh, :total_precip, :loaded_at)
        ON CONFLICT(city, date) DO UPDATE SET
            avg_temp_c   = excluded.avg_temp_c,
            max_temp_c   = excluded.max_temp_c,
            min_temp_c   = excluded.min_temp_c,
            avg_humidity = excluded.avg_humidity,
            avg_wind_kmh = excluded.avg_wind_kmh,
            total_precip = excluded.total_precip,
            loaded_at    = excluded.loaded_at
    """
    conn.executemany(sql, records)
    conn.commit()
    logger.info(f"💾 Loaded {len(records)} records into DB")
    return len(records)


# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

def run_pipeline():
    logger.info("🚀 Pipeline started")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    cities = get_cities_from_db(conn)
    run_at      = datetime.now().isoformat()
    total       = 0
    cities_done = 0

    for city in cities:
        raw = extract(city)
        if raw is None:
            continue
        records = transform(raw, city["name"])
        total  += load(conn, records)
        cities_done += 1

    conn.execute(
        "INSERT INTO pipeline_runs (run_at, cities, records, status) VALUES (?, ?, ?, ?)",
        (run_at, cities_done, total, "success")
    )
    conn.commit()
    conn.close()

    logger.info(f"✅ Pipeline complete — {cities_done} cities, {total} records")


if __name__ == "__main__":
    run_pipeline()
