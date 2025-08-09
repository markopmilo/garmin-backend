"""
All HTTP routes (Blueprint).
"""
from __future__ import annotations
import os
import shutil
import subprocess
from datetime import datetime
from flask import Blueprint, jsonify, request, current_app

from config import (
    UPDATE_LOG, DATA_ROOT, DB_PATH, CFG_PATH, CLI,
    read_cfg, write_cfg, ensure_healthdata_tree, DEFAULT_CFG,
)
from db import fetch_daily_summary, fetch_sleep, fetch_steps, fetch_stress, fetch_exercise

api = Blueprint("api", __name__)

# ---------- tiny helpers ----------

def _json_error(msg: str, status: int = 500):
    return jsonify({"error": msg}), status

def _require_db_exists():
    if not DB_PATH.exists():
        return _json_error(f"Database not found at {DB_PATH}", 503)
    return None

def _run_garmindb():
    """Run garmindb CLI and capture logs."""
    env = {**os.environ, "HOME": str(DATA_ROOT.parent)}
    cmd = [CLI, "-a", "-m", "-s", "--download", "--import", "--analyze", "-l"]

    started = datetime.utcnow()
    cp = subprocess.run(cmd, env=env, capture_output=True, text=True)
    ended = datetime.utcnow()

    with UPDATE_LOG.open("a", encoding="utf-8") as f:
        f.write(f"\n$ {' '.join(cmd)}\n")
        f.write(cp.stdout)
        f.write(cp.stderr)
        f.write(f"\nexit={cp.returncode}\n")

    return {
        "started_at": started.isoformat() + "Z",
        "ended_at": ended.isoformat() + "Z",
        "duration_seconds": (ended - started).total_seconds(),
        "returncode": cp.returncode,
        "ok": (cp.returncode == 0),
        "log": str(UPDATE_LOG),
        "stdout": cp.stdout,
        "stderr": cp.stderr,
    }

# ---------- routes ----------

@api.get("/api/config")
def get_config():
    cfg = read_cfg()
    if "credentials" in cfg and "password" in cfg["credentials"]:
        cfg["credentials"]["password"] = ""
    return jsonify(cfg)

@api.post("/api/config")
def update_config():

    try:
        garth_session = CFG_PATH.parent / "garth_session"
        if garth_session.exists():
            try:
                garth_session.unlink()
            except Exception as e:
                return _json_error(str(e))
    except Exception as e:
        return _json_error(str(e))

    payload = request.get_json(silent=True) or {}
    cfg = read_cfg()

    if "credentials" in payload:
        cfg.setdefault("credentials", {})
        for k in ("user", "password", "secure_password", "password_file"):
            if k in payload["credentials"]:
                cfg["credentials"][k] = payload["credentials"][k]

    if "data" in payload:
        cfg.setdefault("data", {})
        for k in (
            "weight_start_date",
            "sleep_start_date",
            "rhr_start_date",
            "monitoring_start_date",
            "download_latest_activities",
            "download_all_activities",
        ):
            if k in payload["data"]:
                cfg["data"][k] = payload["data"][k]

    if "garmin" in payload and "domain" in payload["garmin"]:
        cfg.setdefault("garmin", {})
        cfg["garmin"]["domain"] = payload["garmin"]["domain"]

    write_cfg(cfg)
    return jsonify({"ok": True})

@api.post("/api/ensure-folders")
def ensure_folders():
    try:
        created = ensure_healthdata_tree()
        wrote_cfg = False
        if not CFG_PATH.exists():
            write_cfg(DEFAULT_CFG.copy())
            wrote_cfg = True
        return jsonify({
            "ok": True,
            "data_root": str(DATA_ROOT),
            "created_paths": created,
            "config_path": str(CFG_PATH),
            "wrote_default_config": wrote_cfg,
        })
    except Exception as e:
        current_app.logger.exception("ensure-folders failed")
        return _json_error(str(e))

@api.post("/api/update")
def update_garmin_data():
    update_config()
    result = _run_garmindb()
    return jsonify(result), (200 if result["ok"] else 500)

@api.get("/api/update/log")
def update_log():
    if UPDATE_LOG.exists():
        return current_app.response_class(UPDATE_LOG.read_text("utf-8"), mimetype="text/plain")
    return "No log yet", 404

@api.get("/api/daily-summary")
def daily_summary():
    guard = _require_db_exists()
    if guard: return guard
    try:
        return jsonify(fetch_daily_summary())
    except Exception as e:
        return _json_error(str(e))

@api.get("/api/stress")
def stress_endpoint():
    guard = _require_db_exists()
    if guard: return guard
    try:
        return jsonify(fetch_stress())
    except Exception as e:
        return _json_error(str(e))

@api.get("/api/steps")
def steps():
    guard = _require_db_exists()
    if guard: return guard
    try:
        return jsonify(fetch_steps())
    except Exception as e:
        return _json_error(str(e))

@api.get("/api/exercise")
def exercise():
    guard = _require_db_exists()
    if guard: return guard
    try:
        return jsonify(fetch_exercise())
    except Exception as e:
        return _json_error(str(e))

@api.get("/api/sleep")
def sleep():
    guard = _require_db_exists()
    if guard: return guard
    try:
        return jsonify(fetch_sleep())
    except Exception as e:
        return _json_error(str(e))

@api.get("/")
def root():
    return jsonify({"ok": True, "msg": "Backend running. Try /api/daily-summary"}), 200

@api.get("/health")
def health():
    return "ok", 200

@api.get("/api/db-info")
def db_info():
    p = DB_PATH
    return jsonify({"db_path": str(p), "exists": p.exists(), "size_bytes": p.stat().st_size if p.exists() else 0})

@api.delete("/api/erase")
def erase_data():
    target = DATA_ROOT
    if not target.exists():
        return _json_error(f"No HealthData folder found at {target}", 503)

    if request.args.get("confirm") != "true":
        return _json_error("You must pass ?confirm=true to erase all data", 400)

    try:
        for item in target.iterdir():
            if item.is_dir(): shutil.rmtree(item)
            else: item.unlink()

        garth_session = CFG_PATH.parent / "garth_session"
        if garth_session.exists():
            try:
                garth_session.unlink()
            except Exception as e:
                return _json_error(str(e))

        return jsonify({"status": "erased_all_contents", "path_cleared": str(target)})
    except Exception as e:
        return _json_error(str(e))
