"""
Flask API — serves pipeline data to the dashboard
New endpoints: add city, remove city, list managed cities
"""

from flask import Flask, jsonify, send_from_directory, request
import sqlite3
import os
import subprocess

app = Flask(__name__, static_folder="static")
DB_PATH = "data/pipeline.db"


def query(sql: str, params: tuple = ()) -> list[dict]:
    if not os.path.exists(DB_PATH):
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/summary")
def summary():
    data = query("""
        SELECT COUNT(DISTINCT city) AS total_cities, COUNT(*) AS total_records,
               ROUND(AVG(avg_temp_c), 1) AS overall_avg_temp, MAX(loaded_at) AS last_run
        FROM weather_daily
    """)
    runs = query("SELECT COUNT(*) as runs FROM pipeline_runs WHERE status='success'")
    result = data[0] if data else {}
    result["pipeline_runs"] = runs[0]["runs"] if runs else 0
    return jsonify(result)


@app.route("/api/cities")
def cities():
    return jsonify(query("""
        SELECT city, date, avg_temp_c, max_temp_c, min_temp_c, avg_humidity, avg_wind_kmh, total_precip
        FROM weather_daily
        WHERE (city, date) IN (SELECT city, MAX(date) FROM weather_daily GROUP BY city)
        ORDER BY avg_temp_c DESC
    """))


@app.route("/api/trend/<city>")
def trend(city: str):
    return jsonify(query("""
        SELECT date, avg_temp_c, max_temp_c, min_temp_c, avg_humidity
        FROM weather_daily WHERE city = ? ORDER BY date ASC
    """, (city,)))


@app.route("/api/runs")
def runs():
    return jsonify(query("""
        SELECT run_at, cities, records, status FROM pipeline_runs ORDER BY run_at DESC LIMIT 10
    """))


@app.route("/api/run", methods=["POST"])
def trigger_run():
    try:
        result = subprocess.run(["python", "etl_pipeline.py"], capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            return jsonify({"status": "success", "log": result.stdout})
        return jsonify({"status": "error", "log": result.stderr}), 500
    except subprocess.TimeoutExpired:
        return jsonify({"status": "error", "log": "Pipeline timed out"}), 500


@app.route("/api/all")
def all_data():
    return jsonify(query("SELECT * FROM weather_daily ORDER BY date DESC, city"))


# ─── CITY MANAGEMENT ENDPOINTS ────────────────────────────────────────────────

@app.route("/api/managed_cities")
def managed_cities():
    """List all cities currently being tracked."""
    return jsonify(query("SELECT name, lat, lon, added_at FROM managed_cities ORDER BY id"))


@app.route("/api/add_city", methods=["POST"])
def add_city():
    """
    Add a new city to the pipeline.
    Body: { "name": "Hyderabad" }  → auto geocodes lat/lon
    Or:   { "name": "Hyderabad", "lat": 17.385, "lon": 78.4867 }
    """
    from etl_pipeline import geocode_city, add_city as etl_add_city, init_db
    data = request.get_json()
    if not data or not data.get("name"):
        return jsonify({"status": "error", "message": "City name required"}), 400

    city_name = data["name"].strip().title()
    lat = data.get("lat")
    lon = data.get("lon")

    # Auto geocode if lat/lon not provided
    if lat is None or lon is None:
        geo = geocode_city(city_name)
        if not geo:
            return jsonify({"status": "error", "message": f"Could not find location for '{city_name}'. Try a different spelling."}), 400
        city_name = geo["name"]
        lat = geo["lat"]
        lon = geo["lon"]

    result = etl_add_city(city_name, lat, lon)
    return jsonify(result), 200 if result["status"] == "success" else 400


@app.route("/api/remove_city", methods=["POST"])
def remove_city():
    """Remove a city from tracking. Body: { "name": "Hyderabad" }"""
    from etl_pipeline import remove_city as etl_remove_city
    data = request.get_json()
    if not data or not data.get("name"):
        return jsonify({"status": "error", "message": "City name required"}), 400

    result = etl_remove_city(data["name"])
    return jsonify(result)


if __name__ == "__main__":
    # Ensure DB and default cities exist on startup
    import os
    os.makedirs("data", exist_ok=True)
    from etl_pipeline import init_db
    import sqlite3 as _sq
    _conn = _sq.connect(DB_PATH)
    init_db(_conn)
    _conn.close()

    print("\n🚀 Server starting at http://localhost:5000\n")
    app.run(debug=True, port=5000)
