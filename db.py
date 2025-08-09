"""
SQLite connection helpers and data fetchers.
"""
from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, List

import pandas as pd

from config import DB_PATH

@contextmanager
def connect():
    con = sqlite3.connect(DB_PATH)
    try:
        yield con
    finally:
        con.close()

def table_exists(con: sqlite3.Connection, name: str) -> bool:
    q = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    return pd.read_sql(q, con, params=(name,)).shape[0] > 0

def get_columns(con: sqlite3.Connection, table: str) -> List[str]:
    try:
        return pd.read_sql(f"PRAGMA table_info({table});", con)["name"].tolist()
    except Exception:
        return []

def _to_seconds(series: pd.Series) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce")
    if num.notna().any():
        return num.astype(float)
    return pd.to_timedelta(series, errors="coerce").dt.total_seconds()

# ------------------------ fetchers ------------------------

def fetch_daily_summary() -> List[Dict[str, Any]]:
    with connect() as con:
        has_sleep = table_exists(con, "sleep_summary")
        has_sleep_seconds = "sleep_seconds" in get_columns(con, "sleep_summary") if has_sleep else False

        if has_sleep and has_sleep_seconds:
            q = """
            SELECT ds.day AS date, ds.steps AS steps, ds.rhr AS restingHeartRate, ss.sleep_seconds AS sleepSeconds
            FROM daily_summary ds LEFT JOIN sleep_summary ss ON ss.day = ds.day
            ORDER BY ds.day DESC
            """
        else:
            q = """
            SELECT day AS date, steps AS steps, rhr AS restingHeartRate, NULL AS sleepSeconds
            FROM daily_summary
            ORDER BY day DESC
            """
        return pd.read_sql(q, con).to_dict(orient="records")

def fetch_sleep() -> List[Dict[str, Any]]:
    with connect() as con:
        if not table_exists(con, "sleep"):
            raise RuntimeError("No 'sleep' table found.")
        q = """
        SELECT day, total_sleep, deep_sleep, light_sleep, rem_sleep, awake,
               avg_spo2, avg_rr, avg_stress, score, qualifier
        FROM sleep ORDER BY day DESC
        """
        df = pd.read_sql(q, con)

    stage_cols = ["total_sleep", "deep_sleep", "light_sleep", "rem_sleep", "awake"]
    for col in stage_cols:
        sec_col = f"{col}_seconds"
        hr_col = f"{col}_hours"
        if col in df.columns:
            secs = _to_seconds(df[col])
            df[sec_col] = secs.astype("Int64")
            df[hr_col] = (secs / 3600.0).round(2)
        else:
            df[sec_col] = pd.Series([pd.NA] * len(df), dtype="Int64")
            df[hr_col] = pd.Series([float('nan')] * len(df), dtype="float")

    df = df.rename(columns={"day": "date"})
    preferred = [
        "date",
        "total_sleep", "total_sleep_seconds", "total_sleep_hours",
        "deep_sleep", "deep_sleep_seconds", "deep_sleep_hours",
        "light_sleep", "light_sleep_seconds", "light_sleep_hours",
        "rem_sleep", "rem_sleep_seconds", "rem_sleep_hours",
        "awake", "awake_seconds", "awake_hours",
        "avg_spo2", "avg_rr", "avg_stress", "score", "qualifier",
    ]
    out = [c for c in preferred if c in df.columns]
    df = df.replace({pd.NA: None, float("nan"): None})
    return df[out].to_dict(orient="records")

def fetch_steps() -> List[Dict[str, Any]]:
    with connect() as con:
        if not table_exists(con, "daily_summary"):
            raise RuntimeError("daily_summary table not found")
        cols = set(get_columns(con, "daily_summary"))
        if not {"day", "steps"}.issubset(cols):
            raise RuntimeError(f"Missing columns in daily_summary: need {{'day','steps'}}, have {cols}")
        extra = ", step_goal" if "step_goal" in cols else ", NULL AS step_goal"
        q = f"SELECT day AS date, steps{extra} FROM daily_summary ORDER BY day DESC"
        return pd.read_sql(q, con).to_dict(orient="records")

def fetch_stress() -> List[Dict[str, Any]]:
    with connect() as con:
        if not table_exists(con, "daily_summary"):
            raise RuntimeError("daily_summary table not found")
        cols = set(get_columns(con, "daily_summary"))
        if not {"day", "stress_avg"}.issubset(cols):
            raise RuntimeError(f"daily_summary missing 'stress_avg' or 'day'. Columns: {cols}")
        q = """
        SELECT day AS date, stress_avg
        FROM daily_summary
        WHERE stress_avg IS NOT NULL
        ORDER BY day DESC
        """
        return pd.read_sql(q, con).to_dict(orient="records")

def fetch_exercise() -> List[Dict[str, Any]]:
    with connect() as con:
        if not table_exists(con, "daily_summary"):
            raise RuntimeError("daily_summary table not found")
        cols = set(get_columns(con, "daily_summary"))
        needed = {"day", "moderate_activity_time", "vigorous_activity_time", "intensity_time_goal"}
        if not needed.issubset(cols):
            raise RuntimeError(f"daily_summary missing time columns: need {needed}, have {cols}")

        select_bits = [
            "day AS date",
            "moderate_activity_time",
            "vigorous_activity_time",
            "intensity_time_goal",
            "distance" if "distance" in cols else "NULL AS distance",
            "calories_active" if "calories_active" in cols else "NULL AS calories_active",
            "calories_total" if "calories_total" in cols else "NULL AS calories_total",
        ]
        q = f"SELECT {', '.join(select_bits)} FROM daily_summary ORDER BY day DESC"
        df = pd.read_sql(q, con)

    # seconds derived from HH:MM:SS
    to_sec = lambda s: pd.to_timedelta(s, errors="coerce").dt.total_seconds()
    df["moderate_activity_seconds"]   = to_sec(df["moderate_activity_time"])
    df["vigorous_activity_seconds"]   = to_sec(df["vigorous_activity_time"])
    df["intensity_time_goal_seconds"] = to_sec(df["intensity_time_goal"])
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
