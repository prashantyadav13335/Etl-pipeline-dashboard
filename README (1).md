# 🌦️ ETL Pipeline Dashboard

A lightweight yet production-patterned **Extract → Transform → Load** pipeline that ingests live weather data for 5 Indian cities, stores it in a structured SQLite warehouse, and visualises it on a real-time dashboard.

> **Stack:** Python · SQL (SQLite) · REST API (Flask) · HTML / CSS / JavaScript  
> **Data Source:** [Open-Meteo API](https://open-meteo.com/) — free, no API key required

---

## 📸 Preview

```
┌─────────────────────────────────────────────────┐
│  ETL_PIPELINE   ●  PIPELINE ACTIVE              │
├──────────┬──────────┬──────────┬────────────────┤
│ RECORDS  │ CITIES   │ AVG TEMP │ PIPELINE RUNS  │
│   45     │    5     │  31.4°   │      3         │
├──────────┴──────────┴──────────┴────────────────┤
│  CITY SNAPSHOT          │  TEMPERATURE TREND    │
│  Delhi      33.2°  ████ │  [Delhi][Mumbai]...   │
│  Mumbai     31.0°  ███  │   ╭──╮                │
│  Chennai    30.5°  ███  │  ╭╯  ╰╮               │
│  Bengaluru  28.1°  ██   │ ╭╯    ╰───            │
│  Kolkata    27.9°  ██   │                       │
└─────────────────────────┴───────────────────────┘
```

---

## 🏗️ Architecture

```
Open-Meteo API
      │
      ▼  EXTRACT
  etl_pipeline.py
      │
      ▼  TRANSFORM
  Clean + Aggregate
  (hourly → daily)
      │
      ▼  LOAD
  SQLite Database
  (data/pipeline.db)
      │
      ▼
  Flask REST API  ──►  Dashboard (index.html)
  (server.py)
```

### Pipeline Stages

| Stage | What happens |
|-------|-------------|
| **Extract** | Fetches 3-day hourly forecasts for 5 cities from Open-Meteo |
| **Transform** | Validates nulls, aggregates hourly → daily (avg/max/min temp, humidity, wind, precipitation) |
| **Load** | Upserts records into SQLite with `ON CONFLICT DO UPDATE` — **idempotent runs** |

---

## 📂 Project Structure

```
etl-pipeline-dashboard/
│
├── etl_pipeline.py       # Core ETL logic (Extract → Transform → Load)
├── server.py             # Flask REST API + static file server
├── index.html            # Live dashboard (HTML/CSS/JS, no framework)
├── requirements.txt      # Python dependencies
│
├── data/
│   └── pipeline.db       # SQLite warehouse (auto-created on first run)
│
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/prashantyadav13335/etl-pipeline-dashboard.git
cd etl-pipeline-dashboard

pip install -r requirements.txt
```

### 2. Run the ETL Pipeline

```bash
python etl_pipeline.py
```

You'll see logs like:
```
2026-04-01 12:00:00 [INFO] 🚀 Pipeline started
2026-04-01 12:00:01 [INFO] ✅ Extracted data for Delhi
2026-04-01 12:00:02 [INFO] 🔄 Transformed 3 daily records for Delhi
2026-04-01 12:00:02 [INFO] 💾 Loaded 3 records into DB
...
2026-04-01 12:00:06 [INFO] ✅ Pipeline complete — 5 cities, 15 records
```

### 3. Start the API Server

```bash
python server.py
```

### 4. Open the Dashboard

Visit **http://localhost:5000** in your browser.

You can also trigger the pipeline directly from the dashboard using the **▶ RUN_PIPELINE.PY** button.

---

## 🔌 API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/summary` | GET | KPI metrics — total records, cities, avg temp, run count |
| `/api/cities` | GET | Latest snapshot per city |
| `/api/trend/<city>` | GET | 3-day temperature trend for a city |
| `/api/runs` | GET | Pipeline run history (last 10) |
| `/api/run` | POST | Trigger ETL pipeline from dashboard |
| `/api/all` | GET | Full raw data from warehouse |

### Example Response — `/api/cities`

```json
[
  {
    "city": "Delhi",
    "date": "2026-04-01",
    "avg_temp_c": 33.2,
    "max_temp_c": 38.5,
    "min_temp_c": 27.1,
    "avg_humidity": 42.0,
    "avg_wind_kmh": 12.3,
    "total_precip": 0.0
  }
]
```

---

## 🗄️ Database Schema

```sql
-- Fact table: aggregated daily weather per city
CREATE TABLE weather_daily (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    city          TEXT    NOT NULL,
    date          TEXT    NOT NULL,         -- YYYY-MM-DD
    avg_temp_c    REAL,
    max_temp_c    REAL,
    min_temp_c    REAL,
    avg_humidity  REAL,                     -- %
    avg_wind_kmh  REAL,
    total_precip  REAL,                     -- mm
    loaded_at     TEXT,                     -- ISO timestamp
    UNIQUE(city, date)                      -- prevents duplicates
);

-- Audit table: pipeline run log
CREATE TABLE pipeline_runs (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at   TEXT NOT NULL,
    cities   INTEGER,
    records  INTEGER,
    status   TEXT                           -- 'success' | 'error'
);

CREATE INDEX idx_city_date ON weather_daily (city, date);
```

---

## ⚙️ Configuration

To add more cities or change the data source, edit the `CITIES` list in `etl_pipeline.py`:

```python
CITIES = [
    {"name": "Hyderabad", "lat": 17.3850, "lon": 78.4867},
    # Add any city with lat/lon coordinates
]
```

---

## 🤖 Automation (Optional)

Schedule the pipeline to run automatically using a cron job:

```bash
# Run every 6 hours
0 */6 * * * cd /path/to/etl-pipeline-dashboard && python etl_pipeline.py
```

Or use Windows Task Scheduler if on Windows.

---

## 💡 Key Concepts Demonstrated

- **ETL pattern** — Extract, Transform, Load with clear separation of concerns
- **Idempotent pipeline** — `ON CONFLICT DO UPDATE` ensures re-runs are safe
- **Data aggregation** — Hourly API data → daily analytical summaries
- **REST API design** — Clean Flask endpoints serving structured JSON
- **Pipeline observability** — Run logging with timestamps, record counts, status
- **Error handling** — Per-city extraction failures don't crash the pipeline

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| ETL Logic | Python stdlib + `requests` |
| Storage | SQLite (via `sqlite3`) |
| API Server | Flask |
| Frontend | Vanilla HTML/CSS/JS |
| Data Source | Open-Meteo API |

---

## 📈 Future Improvements

- [ ] Migrate from SQLite → PostgreSQL for multi-user scenarios
- [ ] Add Snowflake as a target warehouse layer
- [ ] Dockerize for easy deployment
- [ ] Add data quality checks (null %, outlier detection)
- [ ] Email/Slack alerts on pipeline failure

---

## 👤 Author

**Prashant Yadav**  
B.Tech IT @ AIMT-AKTU | Full-Stack & Data Engineering  
[LinkedIn](https://linkedin.com/in/prashant-yadav-76a5b3274) · [GitHub](https://github.com/prashantyadav13335)

---

## 📄 License

MIT — free to use, modify, and distribute.
