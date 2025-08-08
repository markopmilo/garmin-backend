import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS

UPDATE_LOG = Path(__file__).parent / "update.log"

app = Flask(__name__)
CORS(app)

DB_PATH = Path.home() / "HealthData/DBs/garmin.db"

def _run_update_sync():
    env = os.environ.copy()
    env.setdefault("HOME", str(Path.home()))
    cmd = [CLI, "-a", "-m", "-s", "--download", "--import", "--analyze", "-l"]

    started = datetime.utcnow()
    cp = subprocess.run(cmd, env=env, capture_output=True, text=True)
    ended = datetime.utcnow()

    with open(UPDATE_LOG, "a", encoding="utf-8") as f:
        f.write(f"\n$ {' '.join(cmd)}\n")
        f.write(cp.stdout)
        f.write(cp.stderr)
        f.write(f"\nexit={cp.returncode}\n")

    return {
        "started_at": started.isoformat() + "Z",
        "ended_at": ended.isoformat() + "Z",
        "duration_seconds": (ended - started).total_seconds(),
        "returncode": cp.returncode,
        "ok": cp.returncode == 0,
        "log": str(UPDATE_LOG),
        "stdout": cp.stdout,
        "stderr": cp.stderr,
    }

def _table_exists(con, name: str) -> bool:
    q = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    return pd.read_sql(q, con, params=(name,)).shape[0] > 0

def _get_columns(con, table: str):
    try:
        return pd.read_sql(f"PRAGMA table_info({table});", con)["name"].tolist()
    except Exception:
        return []

def _col_exists(con, table: str, col: str) -> bool:
    return col in _get_columns(con, table)

CLI = str(Path(sys.executable).parent / "garmindb_cli.py")

def _run_update():
    env = os.environ.copy()
    env.setdefault("HOME", str(Path.home()))
    # Only activities (-a), monitoring (-m), sleep (-s), latest (-l)
    cmd = [CLI, "-a", "-m", "-s", "--download", "--import", "--analyze", "-l"]
    with open(UPDATE_LOG, "a", encoding="utf-8") as f:
        f.write(f"\n$ {' '.join(cmd)}\n")
        cp = subprocess.run(cmd, env=env, capture_output=True, text=True)
        f.write(cp.stdout)
        f.write(cp.stderr)
        f.write(f"\nexit={cp.returncode}\n")

def fetch_daily_summary():
    con = sqlite3.connect(DB_PATH)
    try:
        if _table_exists(con, "sleep_summary") and _col_exists(con, "sleep_summary", "sleep_seconds"):
            query = """
            SELECT
              ds.day                  AS date,
              ds.steps                AS steps,
              ds.rhr                  AS restingHeartRate,
              ss.sleep_seconds        AS sleepSeconds
            FROM daily_summary ds
            LEFT JOIN sleep_summary ss ON ss.day = ds.day
            ORDER BY ds.day DESC
            LIMIT 30
            """
        else:
            query = """
            SELECT
              day                      AS date,
              steps                    AS steps,
              rhr                      AS restingHeartRate,
              NULL                     AS sleepSeconds
            FROM daily_summary
            ORDER BY day DESC
            LIMIT 30
            """
        df = pd.read_sql(query, con)
        return df.to_dict(orient="records")
    finally:
        con.close()

def _to_seconds(series):
    return pd.to_timedelta(series, errors="coerce").dt.total_seconds()

def fetch_sleep(days: int = 30):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sleep'")
        if not cur.fetchone():
            raise RuntimeError("No 'sleep' table found. Expected columns: day, total_sleep, deep_sleep, light_sleep, rem_sleep, awake")

        q = """
        SELECT
          day,
          total_sleep,
          deep_sleep,
          light_sleep,
          rem_sleep,
          awake,
          avg_spo2,
          avg_rr,
          avg_stress,
          score,
          qualifier
        FROM sleep
        ORDER BY day DESC
        LIMIT ?
        """
        df = pd.read_sql(q, con, params=(days,))

        for col in ["total_sleep", "deep_sleep", "light_sleep", "rem_sleep", "awake"]:
            sec_col = f"{col}_seconds"
            hr_col  = f"{col}_hours"
            secs = _to_seconds(df[col])
            df[sec_col] = secs.astype("Int64")  # keep as nullable int
            df[hr_col] = (secs / 3600.0).round(2)

        df = df.rename(columns={"day": "date"})

        out_cols = [
            "date",
            "total_sleep", "total_sleep_seconds", "total_sleep_hours",
            "deep_sleep",  "deep_sleep_seconds",  "deep_sleep_hours",
            "light_sleep", "light_sleep_seconds", "light_sleep_hours",
            "rem_sleep",   "rem_sleep_seconds",   "rem_sleep_hours",
            "awake",       "awake_seconds",       "awake_hours",
            "avg_spo2", "avg_rr", "avg_stress", "score", "qualifier"
        ]
        return df[out_cols].to_dict(orient="records")
    finally:
        con.close()

def fetch_steps(days: int = 30):
    con = sqlite3.connect(DB_PATH)
    try:
        if not _table_exists(con, "daily_summary"):
            raise RuntimeError("daily_summary table not found")
        cols = _get_columns(con, "daily_summary")
        needed = {"day", "steps"}
        if not needed.issubset(set(cols)):
            raise RuntimeError(f"Missing columns in daily_summary: need {needed}, have {cols}")

        extra = ", step_goal" if "step_goal" in cols else ", NULL AS step_goal"

        q = f"""
        SELECT day AS date, steps{extra}
        FROM daily_summary
        ORDER BY day DESC
        LIMIT ?
        """
        df = pd.read_sql(q, con, params=(days,))
        return df.to_dict(orient="records")
    finally:
        con.close()

def fetch_stress(days: int = 30):
    con = sqlite3.connect(DB_PATH)
    try:
        if not _table_exists(con, "daily_summary"):
            raise RuntimeError("daily_summary table not found")
        cols = _get_columns(con, "daily_summary")
        if "stress_avg" not in cols or "day" not in cols:
            raise RuntimeError(f"daily_summary missing 'stress_avg' or 'day'. Columns: {cols}")

        q = """
        SELECT day AS date, stress_avg
        FROM daily_summary
        WHERE stress_avg IS NOT NULL
        ORDER BY day DESC
        LIMIT ?
        """
        df = pd.read_sql(q, con, params=(days,))
        return df.to_dict(orient="records")
    finally:
        con.close()

def fetch_exercise(days: int = 30):
    con = sqlite3.connect(DB_PATH)
    try:
        if not _table_exists(con, "daily_summary"):
            raise RuntimeError("daily_summary table not found")
        cols = _get_columns(con, "daily_summary")

        needed = {"day", "moderate_activity_time", "vigorous_activity_time", "intensity_time_goal"}
        if not needed.issubset(set(cols)):
            raise RuntimeError(f"daily_summary missing time columns: need {needed}, have {cols}")

        select_bits = [
            "day AS date",
            "moderate_activity_time",
            "vigorous_activity_time",
            "intensity_time_goal",
        ]
        select_bits.append("distance" if "distance" in cols else "NULL AS distance")
        select_bits.append("calories_active" if "calories_active" in cols else "NULL AS calories_active")
        select_bits.append("calories_total" if "calories_total" in cols else "NULL AS calories_total")

        q = f"""
        SELECT {", ".join(select_bits)}
        FROM daily_summary
        ORDER BY day DESC
        LIMIT ?
        """
        df = pd.read_sql(q, con, params=(days,))

        def to_seconds(series):
            # handles 'HH:MM:SS' strings; returns int or NaN
            return pd.to_timedelta(series, errors="coerce").dt.total_seconds()

        df["moderate_activity_seconds"]   = to_seconds(df["moderate_activity_time"])
        df["vigorous_activity_seconds"]   = to_seconds(df["vigorous_activity_time"])
        df["intensity_time_goal_seconds"] = to_seconds(df["intensity_time_goal"])

        df["total_activity_seconds"] = (
            df["moderate_activity_seconds"].fillna(0) + df["vigorous_activity_seconds"].fillna(0)
        ).astype("Int64")

        cols_out = [
            "date",
            "moderate_activity_time", "vigorous_activity_time", "intensity_time_goal",
            "moderate_activity_seconds", "vigorous_activity_seconds", "intensity_time_goal_seconds",
            "total_activity_seconds",
            "distance", "calories_active", "calories_total",
        ]
        return df[cols_out].to_dict(orient="records")
    finally:
        con.close()

@app.post("/api/update")
def update_garmin_data():
    result = _run_update_sync()  # blocks until the CLI finishes
    status_code = 200 if result["ok"] else 500
    return jsonify(result), status_code

@app.get("/api/update/log")
def update_log():
    if UPDATE_LOG.exists():
        return app.response_class(UPDATE_LOG.read_text("utf-8"), mimetype="text/plain")
    return "No log yet", 404

@app.get("/api/daily-summary")
def daily_summary():
    if not DB_PATH.exists():
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 503
    try:
        data = fetch_daily_summary()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/stress")
def stress_endpoint():
    if not DB_PATH.exists():
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 503
    try:
        return jsonify(fetch_stress(30))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/steps")
def steps():
    if not DB_PATH.exists():
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 503
    try:
        return jsonify(fetch_steps(30))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/exercise")
def exercise():
    if not DB_PATH.exists():
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 503
    try:
        return jsonify(fetch_exercise(30))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/sleep")
def sleep():
    if not DB_PATH.exists():
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 503
    try:
        return jsonify(fetch_sleep(days=30))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/")
def root():
    return jsonify({"ok": True, "msg": "Backend running. Try /api/daily-summary"}), 200

@app.get("/health")
def health():
    return "ok", 200

@app.delete("/api/erase")
def erase_data():
    if not DB_PATH.exists():
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 503

    if request.args.get("confirm") != "true":
        return jsonify({"error": "You must pass ?confirm=true to erase data"}), 400

    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cur.fetchall()]

        for table in tables:
            cur.execute(f"DELETE FROM {table}")
        con.commit()
        con.close()

        return jsonify({"status": "erased", "tables_cleared": tables}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
