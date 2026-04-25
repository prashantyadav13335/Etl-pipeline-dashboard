"""
Flask API — serves pipeline data to the dashboard
"""

from flask import Flask, jsonify, send_from_directory
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

if __name__ == "__main__":
    print("\n🚀 Server starting at http://localhost:5000\n")
    app.run(debug=True, port=5000)
